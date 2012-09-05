#!/usr/bin/env ruby
#  coding: utf-8
require 'bundler/setup'
require 'optparse'
require 'csv'
require 'json'
require 'redis'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: unite.rb {options}"

  opts.on('-d VAL', '--directory', "Set target directory")  { |v| options[:t_dir] = v }
  opts.on('-r VAL', '--result', "Set result directory")  { |v| options[:r_dir] = v }
end.parse!(ARGV)

# configure
redis_db = 2
kvs = Redis.new(:db => redis_db)
kvs.flushdb

# directories settings
target_dir = options[:t_dir] ||= "./sampledata"
datafiles = Dir.glob(File.join(target_dir, "/*.json")

# result files setup
r_basedir = options[:r_dir] ||= "./result"
result_dir = File.join(r_basedir, File.expand_path(target_dir).split("/").last)
Dir.mkdir(result_dir) unless File.directory?(result_dir)
rfile = File.join(result_dir, "all_result.csv")
simplefile = File.join(result_dir, "simple_summary.csv")

## map all data to redis hashkeys
datafiles.each do |datafile|
  billing_by_owners = JSON.load(File.read(datafile))
  billing_by_owners.map do |owner, machines|
    machines.each do |machine, machine_data|
      machine_data["metering"]["network"].keys.each do |netx|
        %w(bytes_sent_delta bytes_received_delta).each do |w|
          kvs.hincrby(owner, "#{machine}/#{netx}/#{w}", machine_data["metering"]["network"][netx][w])
        end
      end
    end
  end
end  


csvh = ["owner_uuid", "zone_uuid","net_if","Megabytes_sent_delta","Megabytes_received_delta"]
owner_ids = kvs.keys("????????-????-????-????-????????????")

# all data to csv
CSV.open(rfile, "wb",:force_quotes => true) do |writer|
  writer << csvh
  owner_ids.each do |k_owner|
    kvs.hgetall(k_owner).each do |machine_nif, meter_count|
      zone_uuid, zone_nif, net_vector = machine_nif.split("/")
      next if net_vector == "bytes_received_delta"

      sent_delta = kvs.hget(k_owner, "#{zone_uuid}/#{zone_nif}/bytes_sent_delta").to_i / 1024 / 1024
      received_delta = kvs.hget(k_owner, "#{zone_uuid}/#{zone_nif}/bytes_received_delta").to_i / 1024 / 1024
      writer << [k_owner, zone_uuid, zone_nif, sent_delta, received_delta]
    end
  end
end


csvh = ["owner_uuid", "AllTraffic_by_Gigabytes"]

CSV.open(simplefile, "wb",:force_quotes => true) do |writer|
  writer << csvh
  owner_ids.map do |k_owner|
    traffics = kvs.hvals(k_owner).inject(0) {|sum, i| sum + i.to_i }
    writer << [k_owner, (traffics.to_f / 1024 / 1024 / 1024).round(2)]
  end
end
