#########################################################################################################
# Asker, a slack app
#
# Programmed by: Jeffrey Lau
# Last updated:  10/4/2017
# Function:      this app simulates the functionality of the rhabit web app platform in slack
#
#########################################################################################################

require 'http'
require 'json'
require 'celluloid'
require 'slack-ruby-client'
require 'mysql2'
require 'picky'

db_results = Hash.new		    # this hash will hold the results of the inital database query in the form of [rater] => [[target, question, job_id], [...], ...]

# establish connection to the database
con = Mysql2::Client.new(:host => "localhost", :username => "slack_user", :password => "rhabit123", :database => "slackbot_db")

# query the database to select the data we'll need, the row id, the target, the rater, and the question
rs = con.query(	
'SELECT ask_table.job_id, ask_table.target, questions.question, raters.rater
FROM ask_table
INNER JOIN questions ON ask_table.question_id = questions.question_id
INNER JOIN raters ON ask_table.rater_id = raters.rater_id
WHERE ask_table.response IS NULL')


# loop through the mysql result and put the info into db_results
rs.each do |row|
	if db_results.key?(row["rater"]) == false									# if there isn't an entry for this rater in the hash
		db_results[row["rater"]] = Array.new									# make a new array for this rater
	end																			# else
	db_results[row["rater"]] << [row["target"], row["question"], row["job_id"]]	# add this row to the hash at hash[rater] in the form of a new array
end


# set the OAuth token from the environment variables
Slack.configure do |config|
	config.token = ENV['SLACK_API_TOKEN']
end


# make a new slack web client connection (slack web api)
wclient = Slack::Web::Client.new

ask_queue = Hash.new			# this will hold the queue of jobs for our real time client
channel_ids = Hash.new			# this will translate channel ims from user ids in the form [user_id] => channel_id

channel_list = wclient.im_list	# this is a list of im channels the bot has open (should be one for every member on the slack team)


# populate the channel list hash
channel_list['ims'].each do |this_channel|
	channel_ids[this_channel['user']] = this_channel['id']
end


# loop through the database results and construct our ask_queue
db_results.each do |key, value|																	# for each entry in the hash, key is the rater, value is the outer array (of arrays)										
	value.each do |value2|																		# loop through each inner array (value2)
		this_user = key.dup																		# duplicate the rater so the string is 'unfrozen'
		this_user_data = wclient.users_search(user: this_user)									# search for the rater in the list of users
		if this_user_data['members'].empty? == false											# if the rater is not online, dont add them to the queue
			this_user_id = this_user_data['members'][0]['id']									# get the raters user_id (ex. U6AH1BMMT) in order to find the im channel id from channel_ids hash
			toAdd = [channel_ids[this_user_id], this_user, value2[0], value2[1], value2[2]]		# construct the array to add to the queue in the form [im channel id, rater, target, question, db row id]
			if (ask_queue.key?(this_user) == false)												# if theres no entry in the ask_queue for this rater, make an empty array
				ask_queue[this_user] = Array.new
			end																					
			ask_queue[this_user] << toAdd														# add this line to the raters queue 
		end
	end
end


#####################################################################################################################
# Method: queue_next
# Inputs: ask_queue, a hash in the form of [rater] => [im channel id, rater, target, question, db row id]
#		  responses, a hash in the form of [im channel id] => [queue #, is_waiting?, db_row_id]
#         client, a variable passed in from the slack real time api
#
# Function: finds the next item in each raters queue and asks that rater the question in their im with 
#			this app/bot. Only advances to next item in queue if that rater has responded to any previous questions
#
#####################################################################################################################

def queue_next(ask_queue, responses, client)
	ask_queue.each do |key, value|			# loop through the ask queue, key is rater, value is outer array (of arrays)
		i = 0								# iterator to keep track of inner array
		value.each do |value2|																						# loop through the value, value2, is the inner array (each job)

			if responses.key?(value2[0]) == false || (responses.key?(value2[0]) && 									# if this is either the first job for this rater, or 
													  responses[value2[0]][0] == i && 								# this job number is the question the rater is on and
													  responses[value2[0]][1] == false)								# the bot is not waiting for a previous reply from this rater
				this_question = value2[3].sub('[Target]', value2[2])												# substitute the target into the question text
				if responses.key?(value2[0]) == false																# if this is the first job for this rater														
					client.message channel: value2[0], 																# send them the first job text
					text: "Hi #{value2[1]}, I'd like to ask you a question about #{value2[2]}"
				else																							    # otherwise send them the next job text
					client.message channel: value2[0], 
					text: "#{value2[1]}, I'd like to ask you another question, this one concerning #{value2[2]}"
				end
				
				client.message channel: value2[0], text: "#{this_question}. Type Yes, No, or Unknown"				# send them the actual question
				
				# populate/update the responses hash
				if responses.key?(value2[0]) == false					# if there is no entry for this rater (actually, this channel_id for this rater)												
					responses[value2[0]] = [0, true, value2[4]]			# make a new array for this channel_id
				else													# else
					responses[value2[0]][1] = true						# flag this channel as waiting for a response
					responses[value2[0]][2] = value2[4]					# pass along the db row id
				end
			end
			i += 1														# increment the iterator
			
		end
	end
end

#####################################################################################################################


client = Slack::RealTime::Client.new # establish a new connection to the slack real time api

responses = Hash.new				 # a hash to keep track of the responses for each indivdual rater in the form [im channel id] => [queue #, is_waiting?, db_row_id]

# on startup, call queue_next to get the queue started
client.on :hello do
  puts "Successfully connected, welcome '#{client.self.name}' to the '#{client.team.name}' team at https://#{client.team.domain}.slack.com."
  queue_next(ask_queue, responses, client)
 end
 
# on recieving a message,
client.on :message do |data|
	if responses.key?(data.channel) && responses[data.channel][1]   # if the channel this message was recieved in corresponds to an entry from the response hash
																	# and the bot is waiting for a response,
		case data.text												# accept three answers, yes, no, and unknown
		when 'Yes' || 'yes' then
			responses[data.channel][1] = false						# for each accepted answer, flag the app as no longer waiting for a response
			responses[data.channel][0] += 1						    # and increment the queue number
			client.message channel: data.channel, text: "Response recorded, thank you"
			# record Yes in the database
			rs = con.query("UPDATE ask_table SET response = 'Y' WHERE job_id = #{responses[data.channel][2]}")
		when 'No' || 'no' then
			responses[data.channel][1] = false
			responses[data.channel][0] += 1
			client.message channel: data.channel, text: "Response recorded, thank you"
			# record No in the database
			rs = con.query("UPDATE ask_table SET response = 'N' WHERE job_id = #{responses[data.channel][2]}")
		when 'Unknown' || 'unknown' then
			responses[data.channel][1] = false
			responses[data.channel][0] += 1
			client.message channel: data.channel, text: "Response recorded, thank you"
			# record unknown in the database
			rs = con.query("UPDATE ask_table SET response = 'D' WHERE job_id = #{responses[data.channel][2]}")
			
		# if an unaccepted answer is given, throw a generic response	
		else
			client.message channel: data.channel, text: "I dont understand, I'm sorry, please type Yes, No, or Unknown"
		end
	end
	# if the bot is no longer waiting, it recieved an accpeted response, call ask_queue again to get the next job for this rater (if any)
	if responses.key?(data.channel) && responses[data.channel][1] == false
		queue_next(ask_queue, responses, client)
	end
end

client.on :close do |_data|
  puts "Client is about to disconnect"
end

client.on :closed do |_data|
  puts "Client has disconnected successfully!"
end

client.start!
