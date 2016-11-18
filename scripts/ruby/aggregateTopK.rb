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

fout = open ARGV.last, 'w'

# emulate a max value heap with the input
n=0
heap=[]
files.each do |fin|
  next if fin.eof
  line = fin.readline.chomp
  tokens = line.split(',')
  next if tokens.size != 21
  heap.push [tokens[0].to_i, tokens[1].to_i, n, line]
  n += 1
end

# input files are assumed to be sorted
n = 0
while (true)

  break if (heap.size ==0)

  print "\r#{n/1000000}M" if (n+=1) % 1000000 == 0

  # get next postings
  nextDoc = (heap.min_by { |x| x[1] })[1]
  break if nextDoc >= maxval
  rows = heap.reject { |x| x[1] != nextDoc }
  term = (rows.min_by { |x| x[0] })[0]
  postings = rows.reject { |x| x[0] != term }

  break if postings.size == 0


  row = []
  row[0] = postings[0][0]
  row[1] = postings[0][1]
  for i in 2..20
    row[i] = 0
  end

  postings.each do |p|
    tokens = p[3].split(',').collect { |x| x.to_i }
    for i in 2..20
      row[i] += tokens[i]
    end
  end

  fout.puts row.collect { |x| x.to_s }.join(',')

  # replace the picked item with the next value from that file
  postings.each do |p|
    i = p[2]
    heap[i] = [maxval, maxval, -1, nil]
    next if files[i].eof
    line = files[i].readline.chomp
    tokens = line.split(',')
    next if tokens.size != 21
    heap[i] = [tokens[0].to_i, tokens[1].to_i, i, line]
  end

end
