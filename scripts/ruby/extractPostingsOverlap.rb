#!/usr/bin/ruby

f_baseline = open ARGV[0]
f_in = open ARGV[1]
topk = ARGV[2].to_i
dataset = ARGV[3]
target = ARGV[4]
index_size = ARGV[5]
semantics = ARGV[6]
scoring = ARGV[7]

# load the baseline
baseline = {}
while not f_baseline.eof

  line=f_baseline.readline.chomp.strip
  next if line == ''
  tokens = line.split(',')
  break if tokens.size == 0
  next if tokens.size < 4

  query = tokens[0].to_i
  doc = tokens[1]
  posting = doc + '-' + tokens[2]

  rank = tokens[3].to_i

  if rank <= topk
    baseline[query] = [[], []] if !baseline.has_key? query
    baseline[query][0] << doc
    baseline[query][1] << posting
  end
end
baseline.each_key { |q| baseline[q][0].uniq! }

# load the result set
results = {}
while not f_in.eof
  line=f_in.readline.chomp.strip
  next if line == ''
  tokens = line.split(',')
  break if tokens.size == 0
  next if tokens.size < 4

  query = tokens[0].to_i
  doc = tokens[1]
  posting = doc + '-' + tokens[2]
  rank = tokens[3].to_i

  if rank <= topk
    results[query] = [[], []] if !results.has_key? query
    results[query][0] << doc
    results[query][1] << posting
  end
end
results.each_key { |k| results[k][0].uniq! }

doc_overlap = 0.0
posting_overlap = 0.0

range = 0..(topk-1)

# compute overlap
baseline.each_key do |i|

  if results.has_key? i

    over = (results[i][0][range] & baseline[i][0][0..topk]).size
    tot = (baseline[i][0][range]).size
    doc_overlap += (1.0 * over / tot)

    over = (results[i][1][range] & baseline[i][1][0..topk]).size
    tot = (baseline[i][1][range]).size
    posting_overlap += (1.0 * over / tot)

  end

end

n = baseline.size * 1.0

if (topk == 1000)
  tk = "1k"
else
  tk = "10"
end

# training_set,model,hits_mode,dataset,learned_label,prune_size,query_semantics,bm25_scoring,metric,value,eval_set
# puts "#{topk},#{"%.4f" % (doc_overlap/n)},#{"%.4f" % (posting_overlap/n)},"+dataset.gsub(/-/, ',')
puts "100K_AND,xgboost,ph,#{dataset},#{target},#{index_size},#{semantics},#{scoring},r_kept@#{tk},#{"%.4f" % (doc_overlap/n)},701-750"
puts "100K_AND,xgboost,ph,#{dataset},#{target},#{index_size},#{semantics},#{scoring},p_kept@#{tk},#{"%.4f" % (posting_overlap/n)},701-750"

