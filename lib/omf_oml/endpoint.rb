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
      debug "OML client listening on #{port}"
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
        if (type == 'blob'); type = 'blob64' end # base64 encode blob
        {:name => name.to_sym, :type => type.to_sym}
      end
      schema_desc.insert(0, {:name => :oml_ts, :type => :double})
      schema_desc.insert(1, {:name => :sender_id, :type => :string})
      schema_desc.insert(2, {:name => :oml_seq_no, :type => :integer})
      schema = OMF::OML::OmlSchema.create(schema_desc)
      debug "New schema '#{sname}(#{index})' with schema '#{schema}'"
      @streams[index] = tuple = OmlTuple.new(sname, schema)
      @endpoint.report_new_stream(sname, tuple)
    end

    def parse_meta_row(row)
      puts "META>> #{row.inspect}"
      unless row.length == 5
        warn "Received mis-formatted META tuple - #{row}"
        return
      end
      key = "#{row[2]}/#{row[3]}".downcase
      value = row[4]
      debug "META: '#{key}' - '#{value}'"
      case key
      when './schema'
        parse_schema(value)
      else
        warn "Unknwon META type '#{key}'"
      end
    end

    def parse_rows(socket)
      sender_id = @header['sender-id'] || 'unknown'
      while (l = socket.gets)
        return if l.length == 0

        els = l.strip.split("\t")
        #puts "R(#{sender_id})>> '#{els.inspect}'"
        index = els.delete_at(1).to_i
        if (index == 0)
          parse_meta_row(els)
          next
        end
        unless stream = @streams[index - 1]
          warn "Receiving tuples for unknown schema index '#{index}'"
          next
        end
        els.insert(1, sender_id)
        row = @streams[index - 1].parse_tuple(els)
      end
    end

  end # OMLEndpoint


end

if $0 == __FILE__

  require 'omf_oml/table'
  require 'omf_base/lobject'
  OMF::Base::Loggable.init_log 'endpoint'

  ep = OMF::OML::OmlEndpoint.new(3003)
  ep.on_new_stream() do |name, stream|
    puts "New stream: #{stream}-#{stream.class}"
    # stream.on_new_tuple() do |v|
      # puts "New vector: #{v.select(:a, :f)} - #{v.to_a}"
      # #toml.add_row(v.select(:oml_ts, :value))
    # end
    table = stream.create_table(name + '_tbl', :max_size => 5)
    table.on_content_changed do |action, change|
      puts "TTTT > #{action} - #{change}"
    end
  end
  ep.run(false)

end

