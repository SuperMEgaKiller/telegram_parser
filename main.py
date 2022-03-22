import datetime
import re
import pandas as pd
import config

from telethon import TelegramClient

api_id = 18929800
api_hash = '851296b4c502942e872c4eb6c41d3c5e'

#                       рік  місяць день година
date = datetime.datetime(2022, 3, 21, 17, 0, 0, tzinfo=datetime.timezone.utc)

client = TelegramClient('vladyslav', api_id, api_hash).start(config.phone)

columns = ["message_link", "head", "phone_numbers", "message"]

bad_channels = []
async def main():
  print("start")

  df = pd.read_csv("data.csv", names=columns)
  df.drop(df.index, inplace=True)
  list_of_hashtags = []
  list_of_numbers = []
  list_of_messages = []
  list_of_messages_link = []

  counter = 0
  for channel_url in config.channels:
    try:
      channel = await client.get_input_entity(channel_url)
    except ValueError:
      print("bad channel", channel_url)
      bad_channels.append(channel_url)
      continue
    
    counter = counter + 1
    print(channel_url, counter)
    async for message_obj in client.iter_messages(channel, offset_date=date, reverse=True):
      if(message_obj.message):
        # clear message from '(', ')', '-', '.', '-'
        cleared_message = re.sub(r'[(|)|-|.|-]', '', message_obj.message)
        
        phone_numbers = re.findall(r'\+1.*\d', cleared_message)
        hashtags = re.findall(r'(#+[а-яА-Я0-9(_)]{1,})', cleared_message)
      
        phone_numbers = ' '.join(list(map(lambda phone: phone.replace(" ", ""), phone_numbers)))
        if(phone_numbers and not any(x in hashtags for x in config.bad_hashtags) and phone_numbers not in config.used_numbers):
          # print(phone_numbers, message_obj.date)
          list_of_hashtags.append(hashtags)
          list_of_messages_link.append(f'{channel_url}/{message_obj.id}')
          list_of_numbers.append(phone_numbers)
          list_of_messages.append(message_obj.message)

  df_row = pd.DataFrame({"message_link": list_of_messages_link, "head": list_of_hashtags, "phone_numbers": list_of_numbers, "message": list_of_messages})
  df_row.to_csv("data.csv")
  print("done")
  print(bad_channels)
  
with client:
  client.loop.run_until_complete(main())
