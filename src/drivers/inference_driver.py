import logging
from src.transformers.inference_transformer import InferenceTransformer
from src.utils.utilities import get_spark_session, read_config


class InferenceDriver(InferenceTransformer):
    def __init__(self):
        logging.info("Starting ML Inference Run")

    @staticmethod
    def run(config_file):
        config_dict = read_config(conf_json=config_file)
        spark = get_spark_session(app_name=config_dict.get("app_name", "test"))
        try:
            inference_obj = InferenceTransformer(spark=spark)
            read_data = inference_obj.read_data(path=config_dict["ip_path"])
            model = inference_obj.read_data(path=config_dict["model_path"])
            transformed_df = inference_obj.transform_data(model=model, df=read_data)
            transformed_df.show()
            inference_obj.write_data(write_data=transformed_df,
                                     write_mode=config_dict.get("write_mode", "overwrite"),
                                     path=config_dict["op_path"])

        except Exception as err_msg:
            logging.error(str(err_msg))
            raise err_msg

        finally:
            spark.stop()
