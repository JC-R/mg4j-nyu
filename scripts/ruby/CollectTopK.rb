#!/usr/bin/ruby
#

# consolidate/aggregate hits files
# input files are assumed to be sorted by hits descending
#
# format is assumed: 

# parameters
# arg 0: input file pattern
# arg 1: tot postings
# arg 2: output file

totPostings = ARGV[1].to_i
thresholds = []
files = []
candidates = []
i = 0

# input files(s)
Dir.glob(ARGV[0]).each do |f|
  files.push(open f)
end

fout = open ARGV[2],'w'

# emulate a max value heap with the input
n=0
files.each do |fin|
  next if fin.eof
  line = fin.readline.chomp
  tokens = line.split(',')
  next if tokens.size != 3
  candidates.push [line,n,tokens[2].to_f]
  n += 1
end

# input files are assumed to be sorted by hits descending
while(true)
  print "\r#{n/1000000}M" if n % 1000000 == 0
  n += 1
  break if (candidates.size ==0)

  # pick the next best option (by hits)
  val = candidates.max_by(&:last)
  break if val.last == -1

  fout.puts val[0]

  # replace the picked item with the next value from that file
  i = val[1]
  candidates[i] = ['',0,-1]
  next if files[i].eof
  line = files[i].readline.chomp
  tokens = line.split(',')
  next if tokens.size != 3
  candidates[i] = [line,i,tokens[2].to_f]
end






