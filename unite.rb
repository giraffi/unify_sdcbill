#!/usr/bin/env ruby
#  coding: utf-8
require 'bundler/setup'
require 'optparse'
require 'csv'
require 'json'
require 'redis'
require 'logger'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: unite.rb {options}"

  opts.on('-d VAL', '--directory', "Set target directory")  { |v| options[:t_dir] = v }
  opts.on('-r VAL', '--result', "Set result directory")  { |v| options[:r_dir] = v }
end.parse!(ARGV)

@logger = Logger.new(STDOUT)

## define utils
def error_exit(message)
  @logger.error message; exit(1)
end

class Numeric
  def to_megabytes
    self / 1024 / 1024 
  end
  
  def to_gigabytes
    self / 1024 / 1024 / 1024
  end
end


# Initialize RedisClient
redis_db = 2                    # database number to use.
@logger.info "Create connection to Redis."
begin
  kvs = Redis.new(:db => redis_db)
  kvs.flushdb
rescue Redis::CannotConnectError
  error_exit "redis connection faild. Exit."
end

# csv settings
csvh_all = ["owner_uuid", "zone_uuid","net_if","Megabytes_sent_delta","Megabytes_received_delta"]
csvh_summary = ["owner_uuid", "AllTraffic_by_Gigabytes"]


# target files and directories setup
target_dir = options[:t_dir] ||= "./sampledata"
datafiles = Dir.glob(File.join(target_dir, "/*.json"))

# dir_is_empty?
error_exit "target directory is Empty. Exit." if datafiles == [] 
@logger.info "target files are #{datafiles.join(",")}"

# result files and directories setup
r_basedir = options[:r_dir] ||= "./result"
result_dir = File.join(r_basedir, File.expand_path(target_dir).split("/").last)
Dir.mkdir(result_dir) unless File.directory?(result_dir)
rfile = File.join(result_dir, "all_result_#{File.expand_path(target_dir).split("/").last}.csv")
simplefile = File.join(result_dir, "simple_summary_#{File.expand_path(target_dir).split("/").last}.csv")

## map all data to redis hashkeys
def map_to_rediis(kvs,datafiles)
  @logger.info "start map all data to redis hashkeys"
  datafiles.map do |datafile|
    billing_by_owners = JSON.load(File.read(datafile))
    billing_by_owners.map do |owner, machines|
      @logger.info "  mapping #{owner}"
      begin
        machines.map do |machine, machine_data|
          machine_data["metering"]["network"].keys.each do |netx|
            %w(bytes_sent_delta bytes_received_delta).each do |w|
              kvs.hincrby(owner, "#{machine}/#{netx}/#{w}", machine_data["metering"]["network"][netx][w])
            end
          end if machine_data["metering"]
        end
      rescue => e
        @logger.error e.message
        error_exit "Error detected at map to redis. Exit."
      end
    end
  end  
end

map_to_rediis(kvs,datafiles)
@logger.info "map all data to redis has succeed."

# retreve keys by uuid format
owner_ids = kvs.keys("????????-????-????-????-????????????")


# all data to csv
@logger.info "create all result csv."
CSV.open(rfile, "wb",:force_quotes => true) do |writer|
  writer << csvh_all
  owner_ids.each do |k_owner|
    kvs.hgetall(k_owner).each do |machine_nif, meter_count|
      zone_uuid, zone_nif, net_vector = machine_nif.split("/")
      next if net_vector == "bytes_received_delta"

      sent_delta = kvs.hget(k_owner, "#{zone_uuid}/#{zone_nif}/bytes_sent_delta").to_i.to_megabytes
      received_delta = kvs.hget(k_owner, "#{zone_uuid}/#{zone_nif}/bytes_received_delta").to_i.to_megabytes
      writer << [k_owner, zone_uuid, zone_nif, sent_delta, received_delta]
    end
  end
end

# create simple summary
@logger.info "create simple summary csv."
CSV.open(simplefile, "wb",:force_quotes => true) do |writer|
  writer << csvh_summary
  owner_ids.map do |k_owner|
    traffics = kvs.hvals(k_owner).inject(0) {|sum, i| sum + i.to_i }
    writer << [k_owner, (traffics.to_f.to_gigabytes).round(2)]
  end
end

@logger.info "Done."
