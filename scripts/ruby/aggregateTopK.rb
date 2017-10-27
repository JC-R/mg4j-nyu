#!/usr/bin/ruby
#

# consolidate/aggregate hits files
# input files are assumed to be sorted by doc,term
#
# format is assumed:

# parameters
# arg 0: input file pattern ( a string in quotes *** linux will expand pattern beforehand otherwise)
# arg 2: output file

# input format: term,doc,bin1,bin2,bin3...bin10,bin20,..,bin1280,top10,topk

exit -1 if (ARGV.size != 2)

maxval = 500*1000*1000*1000

# get input files(s)
files = []
Dir.glob(ARGV[0]).each do |f|
  files.push(open f)
end

puts "#{files.size} files"

fout = open ARGV.last,'w'

# emulate a max value heap with the input
n=0
heap=[]
files.each_with_index {|fin, index|
  next if fin.eof
  line = fin.readline.chomp
  tokens = line.split(',')
  next if tokens.size != 19
  heap.push [tokens[0].to_i,tokens[1].to_i,index,line]
}

# input files are assumed to be sorted
n = 0
while(true)

  break if (heap.size ==0)

  print "\r#{n/1000000}M" if (n+=1) % 1000000 == 0

  # get next postings
  term = (heap.min_by{|x| x[0]})[0]
  rows = heap.reject{|x| x[0] != term}
  doc = (rows.min_by{|x| x[1]})[1]
  postings = rows.reject {|x| x[1] != doc}

  break if postings.size == 0

  row = []
  row[0] = term
  row[1] = doc
  for i in 2..20
    row[i] = 0
  end

  postings.each do |p|
    tokens = p[3].split(',').collect {|x| x.to_i}
    # top10
    for i in 2..11
      row[i] += tokens[i]
      row[19] += tokens[i]
    end
    # top1k
    row[20] += row[19]
    for i in 12..18
      row[i] += tokens[i]
      row[20] += tokens[i]
    end
  end

  # replace the picked item with the next value from that file
  postings.each do |p|
    i = p[2]
    heap[i] = [maxval,maxval,-1,nil]
    next if files[i].eof
    line = files[i].readline.chomp
    tokens = line.split(',').collect {|x| x.to_i}
    next if tokens.size != 19
    while (tokens[0] == term && tokens[1] == doc)
      for j in 2..11
        row[j] += tokens[j]
        row[19] += tokens[j]
      end
      # top1k
      row[20] += row[19]
      for j in 12..18
        row[j] += tokens[j]
        row[20] += tokens[j]
      end
      break if files[i].eof
      line = files[i].readline.chomp
      tokens = line.split(',').collect {|x| x.to_i}
      breaK if tokens.size != 19
    end
    next if files[i].eof
    next if tokens.size != 19
    heap[i] = [tokens[0],tokens[1],i,line]

  end
  fout.puts row.collect{|x| x.to_s}.join(',')

end
