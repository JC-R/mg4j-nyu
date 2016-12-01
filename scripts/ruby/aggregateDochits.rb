docs = {}
Dir.glob(ARGV[0]).each do |f|
  fin = open f,'r'
  while not fin.eof
    print "\r#{f} : #{docs.length}" if docs.length % 100000 == 0
    line = fin.readline.chomp
    next if line == ''
    tokens = line.split(',').collect {|x| x.to_i}
    doc = tokens[0]
    if not docs.has_key? doc
      docs[doc] = []
      19.times{|i| docs[doc][i]=0}
    end
    top10 = 0
    top1k = 0
    docs[doc].map!.with_index do |v, i|
      if i<17
        x = v+tokens[i+1]
        top1k += x
        top10 += x if i<10
      elsif i==17
        x = top10
      else
        x = top1k
      end
      x
    end
  end
  puts
end

fout = open ARGV[1],'w'
puts ARGV[1]
docs.each do |k,v|
  fout.print "#{k},"
  fout.puts v.collect{|x| x.to_s}.join(',')
end
