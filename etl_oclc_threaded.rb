require 'thread'
require 'collator'

@fin = open(ARGV.shift) #oclc cluster file
@fout = open(ARGV.shift, 'w') #oclc compiled file
@num_collators = ARGV.shift
@log = open(@num_collators+'.log','w')
@count = 0


start = Time.now
q = Queue.new

foreman = Thread.new do 
  @fin.each_with_index do |line, line_num|
    q << line 
    @count += 1
  end
  Thread.current.join
end


@write_mutex = Mutex.new

collators = []
@num_collators.to_i.times do |x| 
  collators << Thread.new do
    c = Collator.new()
    while foreman.status or !q.empty? 
      if q.empty?
        sleep(5)
      end
      o_str_id, e_str_id, gd_ids = q.pop.chomp.split(/\t/)
      ids = gd_ids.split(',') 

      out_rec = c.build_record(ids).to_json
      @write_mutex.synchronize do 
        @fout.puts out_rec 
      end
    end
    @log.puts 'source_rec: '+c.source_rec_timer.to_s
    @log.puts 'marchash: '+c.marc_hash_timer.to_s
  end
end

collators.each { |thr| thr.join }

duration = Time.now - start
@log.puts "duration: "+duration.to_s
@log.puts "proc time: "+@processing_time.to_s
@log.puts 'per sec: '+(@count/duration).to_s
