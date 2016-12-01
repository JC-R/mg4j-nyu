#!/usr/bin/ruby
#

# consolidate/aggregate raw post hits files
# input files are assumed to be sorted by doc,term
#

# parameters
# arg 0: input file pattern ( a string in *** quotes *** otherwise linux will expand pattern )
# arg 1: output file

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

# input files are assumed to be sorted
# implement a max heap with the first tuple (doc,term) in the line
n=0
heap=[]
files.each do |fin|
  next if fin.eof
  line = fin.readline.chomp
  tokens = line.split(',')
  next if tokens.size < 3   # at least one payload
  tokens.collect! { |x| x.to_i }
  heap.push [tokens[0], tokens[1], n, tokens]
  n += 1
end

n = 0
while (true)

  break if (heap.size ==0)

  print "\r#{n/1000000}M" if (n+=1) % 1000000 == 0

  # get next doc
  nextDoc = (heap.min_by { |x| x[0] })[0]
  break if nextDoc >= maxval

  # find all matching docs in the heaps
  rows = heap.reject { |x| x[0] != nextDoc }

  # get next term
  term = (rows.min_by { |x| x[1] })[1]

  # find the selected posting in the heaps
  postings = rows.reject { |x| x[1] != term }

  break if postings.size == 0

  # aggregate this posting's entries
  rng = 2..(postings[0][3].size-1)
  row = []
  row[0] = postings[0][0]
  row[1] = postings[0][1]
  for i in rng
    row[i] = 0
  end

  postings.each do |p|
    for i in rng
      row[i] += p[3][i]
    end
    # replace the picked item with the next value from that file
    i = p[2]
    heap[i] = [maxval, maxval, -1, nil]
    next if files[i].eof
    line = files[i].readline.chomp
    tokens = line.split(',')
    next if tokens.size < 3
    tokens.collect! { |x| x.to_i }
    heap[i] = [tokens[0], tokens[1], n, tokens]
  end

  #fout.puts row.collect { |x| x.to_s }.join(',')
  puts row.collect { |x| x.to_s }.join(',')

end
