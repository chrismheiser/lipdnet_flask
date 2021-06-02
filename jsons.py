from misc import get_appended_name
from loggers import create_logger

from collections import OrderedDict

logger_jsons = create_logger("jsons")


# IMPORT
def idx_num_to_name(L):
    """
    Switch from index-by-number to index-by-name.

    :param dict L: Metadata
    :return dict L: Metadata
    """
    logger_jsons.info("enter idx_num_to_name")

    try:
        if "paleoData" in L:
            L["paleoData"] = _import_data(L["paleoData"], "paleo")
        if "chronData" in L:
            L["chronData"] = _import_data(L["chronData"], "chron")
    except Exception as e:
        logger_jsons.error("idx_num_to_name: {}".format(e))
        print("Error: idx_name_to_num: {}".format(e))

    logger_jsons.info("exit idx_num_to_name")
    return L


def _import_data(sections, crumbs):
    """
    Import the section metadata and change it to index-by-name.

    :param list sections: Metadata
    :param str pc: paleo or chron
    :return dict _sections: Metadata
    """
    logger_jsons.info("enter import_data: {}".format(crumbs))
    _sections = OrderedDict()
    try:
        for _idx, section in enumerate(sections):
            _tmp = OrderedDict()

            # Process the paleo measurement table
            if "measurementTable" in section:
                _tmp["measurementTable"] = _idx_table_by_name(section["measurementTable"], "{}{}{}".format(crumbs, _idx, "measurement"))

            # Process the paleo model
            if "model" in section:
                _tmp["model"] = _import_model(section["model"], "{}{}{}".format(crumbs, _idx, "model"))

            # Get the table name from the first measurement table, and use that as the index name for this table
            _table_name = "{}{}".format(crumbs, _idx)

            # If we only have generic table names, and one exists already, don't overwrite. Create dynamic name
            if _table_name in _sections:
                _table_name = "{}_{}".format(_table_name, _idx)

            # Put the final product into the output dictionary. Indexed by name
            _sections[_table_name] = _tmp

    except Exception as e:
        logger_jsons.error("import_data: Exception: {}".format(e))
        print("Error: import_data: {}".format(e))

    logger_jsons.info("exit import_data: {}".format(crumbs))
    return _sections


def _import_model(models, crumbs):
    """
    Change the nested items of the paleoModel data. Overwrite the data in-place.

    :param list models: Metadata
    :param str crumbs: Crumbs
    :return dict _models: Metadata
    """
    logger_jsons.info("enter import_model".format(crumbs))
    _models = OrderedDict()
    try:
        for _idx, model in enumerate(models):
            # Keep the original dictionary, but replace the three main entries below

            # Do a direct replacement of chronModelTable columns. No table name, no table work needed.
            if "summaryTable" in model:
                model["summaryTable"] = _idx_table_by_name(model["summaryTable"], "{}{}{}".format(crumbs, _idx, "summary"))
            # Do a direct replacement of ensembleTable columns. No table name, no table work needed.
            if "ensembleTable" in model:
                model["ensembleTable"] = _idx_table_by_name(model["ensembleTable"], "{}{}{}".format(crumbs, _idx, "ensemble"))
            if "distributionTable" in model:
                model["distributionTable"] = _idx_table_by_name(model["distributionTable"], "{}{}{}".format(crumbs, _idx, "distribution"))

            _table_name = "{}{}".format(crumbs, _idx)
            _models[_table_name] = model
    except Exception as e:
        logger_jsons.error("import_model: {}".format(e))
        print("Error: import_model: {}".format(e))
    logger_jsons.info("exit import_model: {}".format(crumbs))
    return _models


def _idx_table_by_name(tables, crumbs):
    """
    Import summary, ensemble, or distribution data.

    :param list tables: Metadata
    :return dict _tables: Metadata
    """
    _tables = OrderedDict()
    try:
        for _idx, _table in enumerate(tables):
            # Use "name" as tableName
            _name = "{}{}".format(crumbs, _idx)
            # Call idx_table_by_name
            _tmp = _idx_col_by_name(_table)
            if _name in _tables:
                _name = "{}_{}".format(_name, _idx)
            _tmp["tableName"] = _name
            _tables[_name] = _tmp
    except Exception as e:
        logger_jsons.error("idx_table_by_name: {}".format(e))
        print("Error: idx_table_by_name: {}".format(e))

    return _tables


def _idx_col_by_name(table):
    """
    Iter over columns list. Turn indexed-by-num list into an indexed-by-name dict. Keys are the variable names.

    :param dict table: Metadata
    :return dict _table: Metadata
    """
    _columns = OrderedDict()

    # Iter for each column in the list
    try:
        for _column in table["columns"]:
            try:
                _name = _column["variableName"]
                if _name in _columns:
                    _name = get_appended_name(_name, _columns)
                _columns[_name] = _column
            except Exception as e:
                print("Error: idx_col_by_name: inner: {}".format(e))
                logger_jsons.info("idx_col_by_name: inner: {}".format(e))

        table["columns"] = _columns
    except Exception as e:
        print("Error: idx_col_by_name: {}".format(e))
        logger_jsons.error("idx_col_by_name: {}".format(e))

    return table
