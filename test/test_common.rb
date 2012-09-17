require 'rubygems'
require 'bud'

$:.unshift File.join(File.dirname(__FILE__), "..")

gem 'minitest'  # Use the rubygems version of MT, not builtin (if on 1.9)
require 'minitest/autorun'
