import logging
from src.dataAccessObjects.DAO import CSVDAO
from src.transformers.library_transformer import LibraryTransformer
from src.utils.utilities import get_spark_session, read_config


class LibraryDriver(LibraryTransformer):
    def __init__(self):
        logging.info("Starting Library Run")

    @staticmethod
    def run(config_file):
        config_dict = read_config(conf_json=config_file)
        spark = get_spark_session(app_name=config_dict.get("app_name", "test"))
        try:
            library_obj = LibraryTransformer(spark=spark)
            read_data = library_obj.read_data(path=config_dict["ip_path"])
            transformed_df = library_obj.transform_data(df=read_data)
            transformed_df.show()
            CSVDAO.write_data(library_obj, write_data=transformed_df,
                              write_mode=config_dict.get("write_mode", "overwrite"),
                              path=config_dict["op_path"])

        except Exception as err_msg:
            logging.error(str(err_msg))
            raise err_msg

        finally:
            spark.stop()
