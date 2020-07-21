import json
from datetime import datetime
import boto3
import pandas as pd

from waterflow.config import OBJECTS, EXP_OBJECTS, EXP_OBJECT_TYPES
from waterflow.utils import NpEncoder

class Tags(object):

    def __init__(self):
        self.queue = {}
        self.cust_queue = {}

    def save(self, artifact, obj: str, dtype: str = None):
        """
        Tags a variable as something to be saved

        Parameters
        ----------
        artifact: variable to store
        obj: str. type of variable to be store.
            Choice of config.OBJECTS
            If you pass a string not in config.OBJECTS
            WaterFlow will store the object under the cust dir
        dtype: type or None, Default None
            used for storing custom variables not in OBJECTS
            Behavior as follows:

            * If `dtype` = 'string': saved to metadata json file as String
            * If `dtype` = 'df': saved as parquet to metadata store provider
            * If `dtype` = 'int' or 'float': saved to metadata json file as int or float
            * If `dtype`= 'viz' or 'other: saved as pickle
    

        Returns
        -------
        `artifact` to preserve declarative interface
        """

        if obj in OBJECTS:
            self.queue[obj] = artifact
        elif not dtype:
            raise ValueError("dtype must be provided if custom obj")
        else:
            self.cust_queue[obj] = (artifact, dtype)

        return artifact

    def ret_queue(self) -> dict:
        return self.queue

    # todo take rows youd like to see as argument
    # todo add shape for pd dataframe
    # todo col for varaible name. %who, local(), dir()
    def inspect(self):
        """
        Returns pd.Dataframe of tagged variables and their values.
        Missing variables will have `NaN` value
        """
        cust_keys = list(self.cust_queue.keys())

        artifact = [self.queue.get(artif) for artif in EXP_OBJECTS] + \
            [self.cust_queue.get(artif)[0] for artif in self.cust_queue]

        types = EXP_OBJECT_TYPES + [self.cust_queue.get(artif)[1] for artif in self.cust_queue]

        return pd.DataFrame({'artifact': artifact, 'type': types}, index=EXP_OBJECTS+cust_keys)

    def flush(self, proj, exp, tag=None):
        """
        Pushes all variables from `queue` to metadata store.
        Generates metadata for artifacts of type pd.Dataframe in JSON
        format

        Parameters
        ----------
        proj: project name on metadata provider
        exp: experiment name
        tag: custom commit message

        """
        # call out s3 service
        # todo create metadata provider file to hook into s3 and blob

        # use datetime as index if tag name not provided
        if not tag:
            tag = str(datetime.utcnow())

        ################
        # Push summary #
        ################

        # create json of columns and types of pandas dfs
        summary = self.inspect()
        # filter for not null df elements
        dfs = summary[(pd.notnull(summary['artifact'])) & (summary['type'] == 'dataframe')]
        df_names = list(dfs.index)

        col_types = {}
        col_stats = {}

        for i in df_names:
            df = dfs['artifact'].loc[i]
            col_types[i] = dict(
                zip(df.columns, df.dtypes.map(lambda x: x.name)))
            col_stats[i] = df.describe().to_dict()

            #############
            # Push dfs #
            #############
            # todo: save largers dfs as parquet
            df.to_csv('s3://{}/{}/{}/{}.csv'.format(proj, exp, tag, i), index=False)

        df_summary = {'types': col_types, 'stats': col_stats}

        s3 = boto3.resource('s3')
        s3object = s3.Object(proj, exp + '/' + tag + '/df_summary.json')

        s3object.put(
            Body=(bytes(json.dumps(df_summary, cls=NpEncoder).encode('UTF-8'))),
            ContentType='application/json'
        )

