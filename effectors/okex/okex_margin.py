import os
import json
import argparse
import logging

import axiom_pkg.gcloud.pubsub as pubsub
import axiom_pkg.gcloud.mle as mle

def env_vars(request):
    os.environ.get('PROJECT_ID', '')
    os.environ.get('MODEL_NAME', '')
    
def is_long(target):
    pass

def executor(data, context):
    config = env_config()
    publisher = pubsub.Publisher()

    """
    {
        "account" : {
            "api_key": "",
            "api_secret": "",
            "passphrase": ""
        },
        "execution" :{
            "id" : 1823983299301,
            "type": "trade", // "flatten"
            "emitted": 11384383974,
            "recieved": 38434703443,
            "actions": [
                {
                    "type": "margin"
                    "base_asset": "ETH",
                    "quote_asset": "BTC",
                    "quantity": 0.1,
                    "bid_price": 0.994,
                    "ask_price": 0.999
                }
            ]
        },
        "instances":[{
            "current_pv": [],
            "feature_frame": []
        }]
    }
    """

    try:
        # parse message
        message = pubsub.parse_message()

        client = okex.OkexMarginClient(
            api_key=message.account.api_key,
            api_secret=message.account.api_secret,
            passphrase=message.account.passphrase,
            use_server_time=True
        )
        
        quote_asset = message.execution.quote_asset
        base_asset = message.execution.base_asset

        # returns min size, size increment, tick_size
        details = client.get_pair_details(
            quote_asset=quote_asset,
            base_asset=base_asset
        ) 

        client.cancel_open_orders(
            quote_asset=quote_asset,
            base_asset=base_asset
        )

        margin_account_settings  = client.get_margin_account_settings(
            quote_asset=quote_asset,
            base_asset=base_asset
        )

        max_quote_quantity = 0
        max_base_quantity = 0
        current_pd = derive_pd(margin_account_settings)

        # get prediction from model 
        action = mle.get_predictions(
            project=config.PROJECT_ID,
            model=config.MODEL_NAME,
            instances=message.instances
        )

        delta = target_pd - current_pd
        quantity = delta * total
        
        # long predicting the base asset price
        # with respect to the quote asset
        # is going to go up
        if is_long(target_pd):
            # make base asset repayments

            # make quote asset orders

            pass  
        # short predicting the base asset price
        # with respect to the quote asset
        # is going to go down
        else:
            # make quote asset repayment

            # make base asset orders

            pass



        # for each action:
            # get margin account of a currency
            # get margin account settings for a currency

            # Calculate the weight distribution for the pair
            # determine the delta between the current portfolio
            # weight and the desired/target portfolio weight
            # taking into account amount in hold, borrowed and 
            # available

            # derive the loans, orders and repayments that need to
            # be made in order to satisfy the portfolio weight
            # multiplied by the bid or ask price respectively

            # place multiple orders

        # persist the results of the execution to output stream

        Output(
            features=[],
            
        )
        
        publisher.publish(
            record="",

        )

    except Exception as e:
        logging.error(e)
        if e == "":
            
            message.ack()