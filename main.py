import datetime
import re
import pandas as pd
import config

from telethon import TelegramClient

date = datetime.datetime(config.year, config.month, config.day, config.hour, 0, 0, tzinfo=datetime.timezone.utc)

client = TelegramClient(config.client, config.api_id,  config.api_hash).start(config.phone)

columns = ["message_link", "head", "phone_numbers", "message"]

bad_channels = []


async def main():
    print("start")

    df = pd.read_csv("data.csv", names=columns)
    df.drop(df.index, inplace=True)
  # move to object
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
        async for message_obj in client.iter_messages(channel,
                                                      offset_date=date,
                                                      reverse=True):
            if (message_obj.message):
                # clear message from '(', ')', '-', '.', '+1'
                cleared_message = re.sub(r'[()\-.]|\+1', '',
                                         message_obj.message)
                # remove spaces
                no_space_message = cleared_message.replace(" ", "")

                # numbers with +1
                # phone_numbers = re.findall(r'\+1.*\d', cleared_message)
              
                # find 10 digits ( probably numbers )
                any_numbers = re.findall(r'\d{10}', no_space_message)
                hashtags = re.findall(r'(#+[а-яА-Я0-9(_)]{1,})',
                                      cleared_message)
                # add +1 to begining
                modified_numbers = [f'+1{s}' for s in any_numbers]

                # print("modified_numbers", modified_numbers)
              
                unique_numbers = list(set(modified_numbers) - set(config.used_numbers))
              
                numbers_to_str = ' '.join(modified_numbers)
              
                if (numbers_to_str
                        and not any(x in hashtags for x in config.bad_hashtags)):
                    list_of_hashtags.append(hashtags)
                    list_of_messages_link.append(
                        f'{channel_url}/{message_obj.id}')
                    list_of_numbers.append(numbers_to_str)
                    list_of_messages.append(cleared_message)

    df_row = pd.DataFrame({
        "message_link": list_of_messages_link,
        "head": list_of_hashtags,
        "phone_numbers": list_of_numbers,
        "message": list_of_messages
    })
    df_row.to_csv("data.csv")
    print("done")
    print('if starts with +11 number is wrong, check it by link')
    print(bad_channels)


with client:
    client.loop.run_until_complete(main())