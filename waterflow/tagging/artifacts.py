import logging
import json
import boto3
import pandas as pd
import pickle

from datetime import datetime
from waterflow.config import OBJECTS, EXP_OBJECTS, EXP_OBJECT_TYPES
from waterflow.utils import NpEncoder

logger = logging.getLogger("tagging_artifact")


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
            * If `dtype` = 'int' or 'float' or 'str': saved to metadata
             json file as int or float or str respectively. Need to retrieve
                entire json file to access any values
            * If `dtype` = 'metric': saved as readily accessible pickle
                value via metadata provider client
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

        artifact = [self.queue.get(artif) for artif in EXP_OBJECTS] + [
            self.cust_queue.get(artif)[0] for artif in self.cust_queue
        ]

        types = EXP_OBJECT_TYPES + [
            self.cust_queue.get(artif)[1] for artif in self.cust_queue
        ]

        return pd.DataFrame(
            {"artifact": artifact, "type": types}, index=EXP_OBJECTS + cust_keys
        )

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
        # todo create metadata provider file to hook into s3 and blob

        # use datetime as index if tag name not provided
        if not tag:
            logger.info("using datetime as tag")
            tag = str(datetime.utcnow())

        #####################
        # generate metadata #
        #####################
        logger.info("generating metadata json file")
        summary = self.inspect()
        # filter for not null df elements
        df_names = list(
            summary[
                (pd.notnull(summary["artifact"])) & (summary["type"] == "dataframe")
            ].index
        )

        col_types_dict = {}
        col_stats_dict = {}

        logger.info("collecting dataframe types and summary stats for json")
        for i in df_names:
            df = summary["artifact"].loc[i]
            col_types_dict[i] = dict(zip(df.columns, df.dtypes.map(lambda x: x.name)))
            col_stats_dict[i] = df.describe().to_dict()

            #############
            # Push dfs #
            #############
            # todo: save larger dfs as parquet, maybe partition as well
            logger.info("saving dataframes as csv to S3")
            df.to_csv("s3://{}/{}/{}/{}.csv".format(proj, exp, tag, i), index=False)

        nums_and_strings = list(
            summary[summary["type"].isin(["int", "float", "str"])].index
        )

        nums_and_strings_dict = {}

        logger.info("collecting nums and strings for json")
        for i in nums_and_strings:
            num_or_str = summary["artifact"].loc[i]
            nums_and_strings_dict[i] = num_or_str

        df_metadata = {
            "types": col_types_dict,
            "stats": col_stats_dict,
            "nums_and_strings": nums_and_strings_dict,
        }

        s3 = boto3.resource("s3")
        s3object = s3.Object(proj, "{}/{}/df_summary.json".format(exp, tag))

        logger.info("pushing metadata json to S3")
        s3object.put(
            Body=(bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))),
            ContentType="application/json",
        )

        logger.info("saving models to s3")
        models = list(summary[summary["type"] == "model"].index)
        for i in models:
            model = summary["artifact"].loc[i]
            pickle_byte_obj = pickle.dumps(model)
            s3.Object(proj, "{}/{}/{}.pkl".format(exp, tag, i)).put(
                Body=pickle_byte_obj
            )
