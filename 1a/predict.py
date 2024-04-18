#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

model = load("1a.joblib")
sys.path.append('.')
from model import fields

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#load the model
model = load("1a.joblib")

#fields = """doc_id,hotel_name,hotel_url,street,city,state,country,zip,class,price,
#num_reviews,CLEANLINESS,ROOM,SERVICE,LOCATION,VALUE,COMFORT,overall_ratingsource""".replace("\n",'').split(",")

#read and infere
read_opts=dict(
        sep='\t', names=[x for x in fields if x !='label'], index_col=False, header=None,
        iterator=True, chunksize=100
)
for df in  pd.read_csv(sys.stdin, **read_opts):
    if len(df) != 0 and any([bool(x) for x in df]):
        pred = list(map(lambda x: x[1], model.predict_proba(df)))
        out = zip(df['id'], pred)
        print("\n".join(["{0}\t{1}".format(*i) for i in out]))
