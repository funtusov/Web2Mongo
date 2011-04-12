# This is a very simple test program that scraps IMDB movies,
# parse it using the HPricot library and saves it in mongodb.
# It uses multiple threads, which help, because the main major bottleneck is i/o (http connection, downloading).
# Thread usage made a 30x increase in running speed on the development computer, on standard ruby VM
#
# It doesn't have any interface really, just edit the source file and run it.
# Call mongo.print_movies to get all the stored movies.
 
class IMDB

  # Connects to IMDB and retrieve the page
  # Uses HPricot library for parsing
  # Returns chosen values

  require 'hpricot'
  require 'open-uri'

  def initialize(url)
    @url = url;
    @hp = Hpricot(open(@url))
  end

  def title
    @title = @hp.at("meta[@name='title']")['content'] # selecting the <meta name="title"> tag content attribute
    @title.gsub!(' - IMDb','') # stripping the ending from the title
    @title
  end

  def rating
    rating_text = (@hp/"span.rating-rating").inner_text # selecting the <span class="rating-ratin"> tag's content
    if rating_text =~ /([\d\.]+)\/10/ # ratings are in form of 8.6/10, striping the last part
      @rating = $1
    end
    @rating
  end

  def director
    @director= (@hp/"div.txt-block a").first.inner_text # selecting <div class="text-box"><a> tag's content
  end

  def imdb_id
    @imdb_id = @hp.at("div.form-box input[@name='title']")['value'] # selecting <div class="form-box"><input name="title"> value attribute
  end
end

class IMDBSaver

  # Connects to mongodb
  # Check whether it was already parsed
  # If not, retrieves movies using IMDB class
  # Then writes in the db

  require 'mongo'
  def initialize
    @db = Mongo::Connection.new.db("database") # connecting to db
    @coll = @db["movies"] # accessing the needed collection / creating new if empty
  end

  def print_movies # a testing method, used to retrieve all entries from the db
    @coll.find().each { |row| puts row.inspect }
  end

  def saver(first,last)
    counter = 0 # initializing counter
    threads = [] # initializing threads
    (first..last).each do |id| # iterating over received id's
        id = "%07d" % id # convert the id in imdb format (177 -> 00000177)
        full_id = "tt#{id}" # adding prefix
        next if @coll.find("imdb_id" => full_id).any? # skipping if the title is already in db
        uri = "http://www.imdb.com/title/#{full_id}/" # building uri
        threads << Thread.new(uri){ |url| # creating new threads
          movie = IMDB.new(url) # instantiating IMDB class
          doc = {"imdb_id" => full_id, "title" => movie.title, "rating" => movie.rating, "director" => movie.director}
          @coll.insert(doc) # inserting into mongo
          counter+=1 # incrementing counter
        }
      end
      threads.each { |athread|  athread.join } # post-work with threads
    puts "#{counter} entries added" # returning number of entries added
  end
end

mongo = IMDBSaver.new # initialize
mongo.saver(1,1000) # set start and finish id's