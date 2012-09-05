#!/usr/bin/env ruby
#  coding: utf-8
require 'bundler/setup'
require 'csv'
require 'json'
require 'redis'

# configure
redis_db = 2


rfile = "result/all_result.csv"
simplefile = "result/simple_summary.csv" 

kvs = Redis.new(:db => redis_db)
kvs.flushdb

datafiles = Dir.glob("./sampledata/*.json")

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
