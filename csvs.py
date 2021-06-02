from inferred_data import get_inferred_data_table
from misc import cast_int, get_missing_value_key, _replace_missing_values_table, rm_missing_values_table, is_ensemble
from loggers import create_logger
import math

logger_csvs = create_logger("csvs")


# MERGE - CSV w/ Metadata


def merge_csv_metadata(d, csvs):
    """
    Using the given metadata dictionary, retrieve CSV data from CSV files, and insert the CSV
    values into their respective metadata columns. Checks for both paleoData and chronData tables.

    :param dict d: Metadata
    :return dict: Modified metadata dictionary
    """
    logger_csvs.info("enter merge_csv_metadata")

    # Add CSV to paleoData
    if "paleoData" in d:
        d["paleoData"] = _merge_csv_section(d["paleoData"], "paleo", csvs)

    # Add CSV to chronData
    if "chronData" in d:
        d["chronData"] = _merge_csv_section(d["chronData"], "chron", csvs)

    logger_csvs.info("exit merge_csv_metadata")
    return d


def _merge_csv_section(sections, pc, csvs):
    """
    Add csv data to all paleo data tables

    :param dict sections: Metadata
    :return dict sections: Metadata
    """
    logger_csvs.info("enter merge_csv_section")

    try:
        # Loop through each table_data in paleoData
        for _name, _section in sections.items():

            if "measurementTable" in _section:
                sections[_name]["measurementTable"] = _merge_csv_table(_section["measurementTable"], pc, csvs)

            if "model" in _section:
                sections[_name]["model"] = _merge_csv_model(_section["model"], pc, csvs)

    except Exception as e:
        print("Error: There was an error merging CSV data into the metadata ")
        logger_csvs.error("merge_csv_section: {}".format(e))

    logger_csvs.info("exit merge_csv_section")
    return sections


def _merge_csv_model(models, pc, csvs):
    """
    Add csv data to each column in chron model

    :param dict models: Metadata
    :return dict models: Metadata
    """
    logger_csvs.info("enter merge_csv_model")

    try:
        for _name, _model in models.items():

            if "summaryTable" in _model:
                models[_name]["summaryTable"] = _merge_csv_table(_model["summaryTable"], pc, csvs)

            if "ensembleTable" in _model:
                models[_name]["ensembleTable"] = _merge_csv_table(_model["ensembleTable"], pc, csvs)

            if "distributionTable" in _model:
                models[_name]["distributionTable"] = _merge_csv_table(_model["distributionTable"], pc, csvs)

    except Exception as e:
        logger_csvs.error("merge_csv_model: {}",format(e))

    logger_csvs.info("exit merge_csv_model")
    return models


def _merge_csv_table(tables, pc, csvs):

    try:
        logger_csvs.info("enter merge table csv")
        for _name, _table in tables.items():
            # Get the filename of this table
            filename = _get_filename(_table)
            ensemble = False
            logger_csvs.info("got filename")
            # If there's no filename, bypass whole process because there's no way to know which file to open
            if not filename:
                logger_csvs.info("Error: merge_csv_column: No filename found for table")
            else:
                # Call read_csv_to_columns for this filename. csv_data is list of lists.
                _one_csv = csvs[filename]
                logger_csvs.info("got one csv")

                # If all the data columns are non-numeric types, then a missing value is not necessary
                _only_numerics = _is_numeric_data(_one_csv)

                if not _only_numerics:
                    logger_csvs.info("not only numerics")
                    # Get the Missing Value key from the table-level data
                    _mv = get_missing_value_key(_table)
                    if _mv:
                        logger_csvs.info("if mv")
                        # Use the Missing Value key to replace all current missing values with "nan"
                        _one_csv = _replace_missing_values_table(_one_csv, _mv)
                    else:
                        logger_csvs.info("No missing value found. You may encounter errors with this data.")
                # Merge the values into the columns
                logger_csvs.info("start merging column")
                _table, ensemble = _merge_csv_column(_table, _one_csv)
                # Remove and missing values keys that are at the column level
                logger_csvs.info("rm missing values")
                _table = rm_missing_values_table(_table)
                # Now put the missing value as "nan" (standard)
                _table["missingValue"] = "nan"

            if not ensemble:
                logger_csvs.info("start inferred data calcs")
                # calculate inferred data before leaving this section! paleo AND chron tables
                _table = get_inferred_data_table(_table, pc)

            tables[_name] = _table
    except Exception as e:
        print("Error: merge_csv_table: {}, {}".format(pc, e))
        logger_csvs.error("merge_csv_table: {}, {}".format(pc, e))
    logger_csvs.info("exit merge tables")
    return tables


def _merge_csv_column(table, csvs):
    """
    Add csv data to each column in a list of columns

    :param dict table: Table metadata
    :param str crumbs: Hierarchy crumbs
    :param str pc: Paleo or Chron table type
    :return dict: Table metadata with csv "values" entry
    :return bool ensemble: Ensemble data or not ensemble data
    """

    # Start putting CSV data into corresponding column "values" key
    try:
        ensemble = is_ensemble(table["columns"])
        if ensemble:
            # realization columns
            if len(table["columns"]) == 1:
                for _name, _column in table["columns"].items():
                    _column["values"] = csvs
            # depth column + realization columns
            elif len(table["columns"]) == 2:
                _multi_column = False
                for _name, _column in table["columns"].items():
                    if isinstance(_column["number"], (int, float)):
                        col_num = cast_int(_column["number"])
                        _column['values'] = csvs[col_num - 1]
                    elif isinstance(_column["number"], list):
                        if _multi_column:
                            raise Exception("Error: merge_csv_column: This jsonld metadata looks wrong!\n"
                                  "\tAn ensemble table depth should not reference multiple columns of CSV data.\n"
                                  "\tPlease manually fix the ensemble columns in 'metadata.jsonld' inside of your LiPD file.")
                        else:
                            _multi_column = True
                            _column["values"] = csvs[2:]
        else:
            for _name, _column in table['columns'].items():
                col_num = cast_int(_column["number"])
                _column['values'] = csvs[col_num - 1]
    except IndexError:
        logger_csvs.warning("merge_csv_column: IndexError: index out of range of csv_data list")
    except KeyError:
        logger_csvs.error("merge_csv_column: KeyError: missing columns key")
    except Exception as e:
        logger_csvs.error("merge_csv_column: Unknown Error:  {}".format(e))
        print("Quitting...")
        exit(1)

    # We want to keep one missing value ONLY at the table level. Remove MVs if they're still in column-level
    return table, ensemble


def _is_numeric_data(ll):
    """
    List of lists of csv values data
    :param list ll:
    :return bool: True, all lists are numeric lists, False, data contains at least one numeric list.
    """
    for l in ll:
        try:
            if any(math.isnan(float(i)) or isinstance(i, str) for i in l):
                return False
            # if not all(isinstance(i, (int, float)) or math.isnan(float(i)) for i in l):
            #     # There is an entry that is a non-numeric entry in this list
            #     return False
        except ValueError:
            # Trying to case a str as a float didnt work, and we got an error
            return False
    # All arrays are 100% numeric or "nan" entries.
    return True

def _get_filename(table):
    """
    Get the filename from a data table. If it doesn't exist, create a new one based on table hierarchy in metadata file.
    format: <dataSetName>.<section><idx><table><idx>.csv
    example: ODP1098B.Chron1.ChronMeasurementTable.csv

    :param dict table: Table data
    :param str crumbs: Crumbs
    :return str filename: Filename
    """
    try:
        filename = table["filename"]
    except KeyError:
        logger_csvs.info("get_filename: KeyError: missing filename for a table")
        print("Error: Missing filename for a table")
        filename = ""
    except Exception as e:
        logger_csvs.error("get_filename: {}".format(e))
        filename = ""
    return filename