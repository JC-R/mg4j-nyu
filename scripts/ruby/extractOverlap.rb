f_baseline = open ARGV[0]
q_size = ARGV[1].to_i
r_size = ARGV[2].to_i
f_in = open ARGV[3]

dataset = ARGV[4]
train = ARGV[5]
model = ARGV[6]
target = ARGV[7]
index_size = ARGV[8]
semantics = ARGV[9]
scoring = ARGV[10]

# load the baseline
n = 0
last = -1
baseline = Array.new(q_size) { |a| Array.new() }
while not f_baseline.eof
	line=f_baseline.readline.chomp.strip
	next if line == ''
  tokens = line.split(' ')
	break if n>=q_size
  query = tokens[0].to_i
  last = query if last == -1
  n += 1 if query != last
  last = query
	baseline[n] << tokens[2]
end
# q_size.times do |q|
# 	puts baseline[q].size
# end

# load the result set
n = 0
last = -1
results = Array.new(q_size) { |a| Array.new() }
while not f_in.eof
	line=f_in.readline.chomp.strip
	if line == ''
    n += 1
    next
  end
	break if n>=q_size
  tokens = line.split(' ')
	results[n] << tokens[1]
end
 # q_size.times do |q|
 # 	puts results[q].size
 # end

# compute overlap 

overlap10 = 0.0
overlap100 = 0.0
overlap1k = 0.0

q_size.times do |i|
  over = (results[i][0..9] & baseline[i][0..9]).size
  tot = (baseline[i][0..9]).size
	overlap10 += (1.0 * over / tot)

	over = (results[i][0..99] & baseline[i][0..99]).size
	tot = (baseline[i][0..99]).size
	overlap100 += (1.0 * over / tot)

  over = (results[i] & baseline[i]).size
	tot = (baseline[i]).size
	overlap1k += (1.0 * over / tot)
end

#puts "#{overlap10},#{overlap100},#{overlap1k},#{tot10},#{tot100},#{tot1k},#{"%.4f" % ((1.0 * overlap10)/tot10)},#{"%.4f" % ((1.0 * overlap100)/tot100)},#{"%.4f" % ((1.0 * overlap1k)/tot1k)},#{dataset},#{train},#{model},#{target},#{index_size},overlap,#{semantics}"
puts "#{model},#{dataset},#{target},#{index_size},#{semantics},#{scoring},overlap@10,#{"%.4f" % (overlap10/n)}"
puts "#{model},#{dataset},#{target},#{index_size},#{semantics},#{scoring},overlap@100,#{"%.4f" % (overlap100/n)}"
puts "#{model},#{dataset},#{target},#{index_size},#{semantics},#{scoring},overlap@1k,#{"%.4f" % (overlap1k/n)}"
