#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_base/lobject'
require 'omf_oml'

require 'omf_oml/oml_tuple'

module OMF::OML

  # This class parses an OML network stream and creates various OML mstreams which can
  # be visualized. After creating the object, the @run@ method needs to be called to
  # start processing the stream.
  #
  class OmlEndpoint < OMF::Base::LObject

    # Register a proc to be called when a new stream was
    # discovered on this endpoint.
    #
    def on_new_stream(key = :_, &proc)
      if proc
        @on_new_stream_procs[key] = proc
      else
        @on_new_stream_procs.delete key
      end
    end

    def initialize(port = 5000, host = "0.0.0.0")
      require 'socket'
      @serv = TCPServer.new(host, port)
      @running = false
      @on_new_stream_procs = {}
    end

    def report_new_stream(name, stream)
      @on_new_stream_procs.each_value do |proc|
        proc.call(name, stream)
      end
    end

    def run(in_thread = true)
      if in_thread
        Thread.new do
          _run
        end
      else
        _run
      end
    end

    private

    def _run
      @running = true
      while @running do
        sock = @serv.accept
        debug "OML client connected: #{sock}"

        Thread.new do
          begin
            conn = OmlSession.new(self)
            conn.run(sock)
            debug "OML client disconnected: #{sock}"
          rescue Exception => ex
            error "Exception: #{ex}"
            debug "Exception: #{ex.backtrace.join("\n\t")}"
          ensure
            sock.close
          end
        end

      end
    end
  end

  # PRIVATE
  # An instance of this class is created by +OmlEndpoint+ to deal with
  # and individual client connection (socket). An EndPoint is creating
  # and instance and then immediately calls the +run+ methods.
  #
  #
  class OmlSession < OMF::Base::LObject  # :nodoc

    # Return the value for the respective @key@ in the protocol header.
    #
    def [](key)
      @header[key]
    end

    def initialize(endpoint)
      @endpoint = endpoint
      @header = {}
      @streams = []
      @on_new_stream_procs = {}
    end

    # This methods blocks until the peer disconnects. Each new stream is reported
    # to the @reportProc@
    #
    def run(socket)
      parse_header(socket)
      parse_rows(socket)
    end

    private
    def parse_header(socket, &reportStreamProc)
      while (l = socket.gets.strip)
        #puts "H>> '#{l}'"        
        return if l.length == 0

        key, *value = l.split(':')
        if (key == 'schema')
          parse_schema(value.join(':'))
        else
          @header[key] = value[0].strip
          debug "HEADER: #{key}: #{@header[key]}"
        end
      end
    end

    def parse_schema(desc)
      debug "SCHEMA: #{desc}"
      els = desc.split(' ')
      #puts "ELS: #{els.inspect}"
      index = els.shift.to_i - 1
      sname = els.shift
      schema_desc = els.collect do |el|
        name, type = el.split(':')
        {:name => name.to_sym, :type => type.to_sym}
      end
      schema_desc.insert(0, {:name => :oml_ts, :type => :double})
      schema_desc.insert(1, {:name => :sender_id, :type => :string})
      schema_desc.insert(2, {:name => :oml_seq_no, :type => :integer})            
      schema = OMF::OML::OmlSchema.create(schema_desc)
      @streams[index] = tuple = OmlTuple.new(sname, schema)
      @endpoint.report_new_stream(sname, tuple)
    end

    def parse_rows(socket)
      sender_id = @header['sender-id'] || 'unknown'
      while (l = socket.gets)
        return if l.length == 0

        els = l.strip.split("\t")
        #puts "R>> '#{els.inspect}'"
        index = els.delete_at(1).to_i - 1
        els.insert(1, sender_id)
        row = @streams[index].parse_tuple(els)
      end
    end

  end # OMLEndpoint


end

if $0 == __FILE__

  require 'omf_oml/table'
  ep = OMF::OML::OmlEndpoint.new(3000)
  toml = OMF::OML::OmlTable.new('oml', [[:x], [:y]], :max_size => 20)
  ep.on_new_stream() do |s|
    puts "New stream: #{s}"
    s.on_new_vector() do |v|
      puts "New vector: #{v.select(:oml_ts, :value).join('|')}"
      toml.add_row(v.select(:oml_ts, :value))
    end
  end
  ep.run(false)

end

