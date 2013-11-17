#-------------------------------------------------------------------------------
# Copyright (c) 2013 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_base/lobject'
require 'omf_oml'

module OMF::OML

  # This class represents the schema of an OML measurement stream.
  #
  class OmlSchema < OMF::Base::LObject

    CLASS2TYPE = {
      TrueClass => 'boolean',
      FalseClass => 'boolean',
      String => 'string',
      Symbol => 'string',
      Fixnum => 'decimal',
      Float => 'double',
      Time => 'dateTime'
    }

    # Map various type definitions (all lower case) into a single one
    ANY2TYPE = {
      'integer' => :integer,
      'int' => :integer,
      'int32' => :integer,
      'int64' => :integer,
      'bigint' => :integer,
      'smallint' => :integer,
      'unsigned integer' => :integer,
      'unsigned bigint' => :integer,
      'float' => :float,
      'real' => :float,
      'double' => :float,
      'double precision' => :float,
      'text' => :string,
      'string' => :string,
      'varargs' => :string,
      'date' => :date,
      'dateTime'.downcase => :dateTime, # should be 'datetime' but we downcase the string for comparison
      'timestamp' => :dateTime, # Postgreql specific, not sure if this works as it.
      'key' => :key,
      'bool' => :bool,
      'blob' => :blob,
      'blob64' => :blob64 # base64 encoded blob
    }

    OML_INTERNALS = [
      :oml_sender,
      :oml_sender_id,
      :oml_seq,
      :oml_ts_client,
      :oml_ts_server
    ]

    def self.create(schema_description)
      if schema_description.kind_of? self
        return schema_description
      end
      return self.new(schema_description)
    end

    # Return the col name at a specific index
    #
    def name_at(index)
      unless d = @schema[index]
        raise "Index '#{index}' out of bounds"
      end
      d[:name]
    end

    # Return the col index for column named +name+
    #
    def index_for_col(name)
      if name.is_a? Integer
        # seems they already know hte index
        index = name
        if (index >= @schema.length)
          raise "Index '#{index}' points past schema - #{schema.length}"
        end
        return index
      end
      name = name.to_sym
      @schema.each_with_index do |col, i|
        return i if col[:name] == name
      end
      raise "Unknown column '#{name}'"
    end

    # Return the column names as an array
    #
    def names
      @schema.collect do |col| col[:name] end
    end

    # Return the col type at a specific index
    #
    def type_at(index)
      @schema[index][:type]
    end

    def columns
      @schema
    end

    def insert_column_at(index, col)
      @schema.insert(index, _create_col_descr(col))
    end

    def replace_column_at(index, col)
      @schema[index] = _create_col_descr(col)
    end

    def each_column(&block)
      @schema.each do |c|
        block.call(c)
      end
    end

    # Translate a record described in a hash of 'col_name => value'
    # to a row array. Note: Will suppress column '__id__'
    #
    # hrow - Hash describing a row
    # set_nil_when_missing - If true, set any columns not described in hrow to nil
    #
    def hash_to_row(hrow, set_nil_when_missing = false, call_type_conversion = false)
      #puts "HASH2A => #{hrow.inspect}"
      remove_first_col = false
      r = @schema.collect do |cdescr|
        cname = cdescr[:name]
        if cname == :__id__
          remove_first_col = true
          next nil
        end
        unless (v = hrow[cname]) ||  (v = hrow[cname.to_sym])
          next nil if set_nil_when_missing
          raise "Missing record element '#{cname}' in record '#{hrow}' - schema: #{@schema}"
        end
        call_type_conversion ? cdescr[:type_conversion].call(v) : v
      end
      r.shift if remove_first_col # remove __id__ columns
      #puts "#{r.inspect} -- #{@schema.map {|c| c[:name]}.inspect}"
      r
    end

    # Cast each element in 'row' into its proper type according to this schema
    #
    def cast_row(raw_row, ignore_first_column = false)
      start_index = ignore_first_column ? 1 : 0
      unless raw_row.length == @schema.length - start_index
        raise "Row needs to have same size as schema (#{raw_row.inspect}-#{describe}) "
      end
      # This can be done more elegantly in 1.9
      row = []
      raw_row.each_with_index do |el, i|
        row << @schema[i + start_index][:type_conversion].call(el)
      end
      row
    end

    # Cast a named column from a 'raw' row into its proper type according to this schema
    #
    def cast_col(col_name, raw_row)
      index = index_for_col(col_name)
      unless raw_value = raw_row[index]
        raise "Row doesn't include a value at '#{index}' (#{raw_row.inspect}-#{describe}) "
      end
      value = @schema[index][:type_conversion].call(raw_value)
    end

    def describe
      @schema.map {|c| {:name => c[:name], :type => c[:type], :title => c[:title] }}
    end

    def to_json(*opt)
      describe.to_json(*opt)
    end

    def clone(exclude_oml_internals = false)
      c = self.dup
      schema = @schema.clone
      if exclude_oml_internals
        schema = schema.select do |cd|
          not OML_INTERNALS.include?(cd[:name])
        end
      end
      c.instance_variable_set('@schema', schema)
      c
    end

    def to_s
      "OmlSchema: #{@schema.map {|c| "#{c[:name]}:#{c[:type]}"}.join(', ')}"
    end

    protected

    # schema_description - Array containing [name, type*] for every column in table
    #   TODO: define format of TYPE
    #
    def initialize(schema_description)
      # check if columns are described by hashes or 2-arrays
      @schema = []
      schema_description.each_with_index do |cdesc, i|
        insert_column_at(i, cdesc)
      end
      debug "schema: '#{describe.inspect}'"

    end

    # Create a column descriptor from whatever is given by user
    #
    def _create_col_descr(col)
      if col.kind_of?(Symbol) || col.kind_of?(String)
        col = [col]
      end
      if col.kind_of? Array
        # should be [name, type]
        if col.length == 1
          col = {:name => col[0].to_sym,
                  :type => :string,
                  :title => col[0].to_s.split('_').collect {|s| s.capitalize}.join(' ')}
        elsif col.length == 2
          col = {:name => col[0].to_sym,
                  :type => col[1].to_sym,
                  :title => col[0].to_s.split('_').collect {|s| s.capitalize}.join(' ')}
        elsif col.length == 3
          col = {:name => col[0].to_sym, :type => col[1].to_sym, :title => col[2]}
        else
          throw "Simple column schema should consist of [name, type, title] array, but found '#{col.inspect}'"
        end
      elsif col.kind_of? Hash
        # ensure there is a :title property
        unless col[:title]
          col[:title] = col[:name].to_s.split('_').collect {|s| s.capitalize}.join(' ')
        end
      end

      # should normalize type
      if type = col[:type]
        tn = type.to_s.split('(')[0] # take care of types like varargs(..)
        unless type = ANY2TYPE[type.to_s.downcase]
          warn "Unknown type definition '#{tn}', default to 'string'"
          type = :string
        end
      else
        warn "Missing type definition in '#{col[:name]}', default to 'string' (#{col.inspect})"
        type = :string
      end
      col[:type] = type

      col[:type_conversion] = case type
        when :string
          lambda do |r| r end
        when :key
          lambda do |r| r end
        when :integer
          lambda do |r| r.to_i end
        when :float
          lambda do |r| r.to_f end
        when :bool
          lambda do |r| (r.downcase.start_with? 't') ? true : false end
        when :date
          lambda do |r| Date.parse(r) end
        when :dateTime
          lambda do |r| Time.parse(r) end
        when :blob64
          require "base64"
          lambda do |r| Base64.decode64(r) end
        else raise "Unrecognized Schema type '#{type}'"
      end
      col
    end
  end # OmlSchema

end
