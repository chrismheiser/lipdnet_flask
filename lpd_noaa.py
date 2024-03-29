import re
from collections import defaultdict, OrderedDict
import os
# NOAA SECTIONS is for writing keys in order, and for mapping keys to their proper sections
# NOAA KEYS is for converting keys from LPD to NOAA
from alternates import NOAA_KEYS_BY_SECTION, LIPD_NOAA_MAP_FLAT, LIPD_NOAA_MAP_BY_SECTION
from misc import clean_doi, generate_timestamp, get_authors_as_str
from loggers import create_logger
logger_lpd_noaa = create_logger("LPD_NOAA")

class LPD_NOAA(object):
    """
    Creates a NOAA object that contains all the functions needed to write out a LiPD file as a NOAA text file.
    Supports
    LiPD Version: v1.2
    NOAA txt template: v3.0

    :return none: Writes NOAA text to file in local storage
    """

    def __init__(self, D, dsn, project, version, path):
        """

        :param dict D: Metadata
        """
        self.path = path
        # LiPD dataset name
        self.dsn = dsn
        self.project = project
        self.version = version
        self.current_yr = 2017
        # Dataset name with LiPD extension
        self.filename_lpd = dsn + ".lpd"
        # Dataset name with TXT extension
        self.filename_txt = dsn + '.txt'
        # List of filenames to write out. One .txt file per data table set.
        self.output_filenames = []
        # Amount of .txt files to be written
        self.output_file_ct = 0
        # The current .txt file writer object that is open.
        self.noaa_txt = None
        # Data_Type (formerly archiveType) for this dataset, used in multiple locations
        self.lsts_tmp = {
            "Data_Type": [],
            "genus": [],
            "species": [],
            "qc": []
        }
        # NOAA url, to landing page where this dataset will be stored on NOAA's servers
        # self.noaa_url = "https://www1.ncdc.noaa.gov/pub/data/paleo/pages2k/{}-{}/data-version-{}/{}".format(project, version, self.current_yr, self.filename_txt)
        self.noaa_url = ""
        # List of all DOIs found in this dataset
        self.doi = []
        # Avoid writing identical pub citations. Store here as intermediate check.
        self.data_citation = {}
        # noaa dict has all metadata with noaa keys
        self.noaa_data = D
        # the raw dictionary imported from jsonld file
        self.lipd_data = D
        # jsonld data sorted according to noaa d sections
        self.noaa_data_sorted = {
            "Top": {},
            "Contribution_Date": {},
            "File_Last_Modified_Date": {},
            "Title": {},
            "Investigators": {},
            "Description_Notes_and_Keywords": {},
            "Publication": [],
            "Funding_Agency": [],
            "Site_Information": {},
            "Data_Collection": {},
            "Species": {},
            # these are sections, but i'm approaching them differently instead of storing them in the sorted data dict.
            # "Chronology_Information": {},
            # "Variables": {},
            # store all table PAIRS here. (chron + paleo @ same idx)
            "Data": [],
            # These are keys that do not currently have NOAA - LPD mappings
            "Ignore": {},
        }
        self.noaa_geo = {
        }
        # Paleo and Chron tables
        self.data_paleos = []
        self.data_chrons = []
        # Append filenames or not? Multiple output
        self.append_filenames = False
        self.noaa_file_output = {}

    # MAIN

    def main(self):
        """
        Load in the template file, and run through the parser
        :return none:
        """
        logger_lpd_noaa.info("enter main")
        # Starting Directory: dir_tmp/dir_bag/data/

        try:
            # convert all lipd keys to noaa keys
            # timestamp the conversion of the file

            # MISC SETUP FUNCTIONS
            logger_lpd_noaa.info("Get timestamp")
            self.noaa_data_sorted["File_Last_Modified_Date"]["Modified_Date"] = generate_timestamp()
            logger_lpd_noaa.info("Count data tables")
            self.__get_table_count()
            # Get measurement tables from metadata, and sort into object self
            logger_lpd_noaa.info("Sort tables")
            self.__put_tables_in_self(["paleo", "paleoData", "measurementTable"])
            self.__put_tables_in_self(["chron", "chronData", "measurementTable"])

            # how many measurement tables exist? this will tell use how many noaa files to create
            logger_lpd_noaa.info("Make table pairs")
            self.__get_table_pairs()

            # reorganize data into noaa sections
            logger_lpd_noaa.info("Map the LiPD data into the NOAA sections")
            self.__reorganize()

            # special case: earliest_year, most_recent_year, and time unit
            # self.__check_time_values()
            # self.__check_time_unit()

            logger_lpd_noaa.info("Get overall data")
            self.__get_overall_data(self.lipd_data)
            logger_lpd_noaa.info("Reorganize sensor")
            self.__reorganize_sensor()
            logger_lpd_noaa.info("Convert lists to strings")
            self.__lists_to_str()
            logger_lpd_noaa.info("Create study name")
            self.__generate_study_name()

            # END MISC SETUP FUNCTIONS

            # Use data in steps_dict to write to
            # self.noaa_data_sorted = self.__key_conversion(self.noaa_data_sorted)
            logger_lpd_noaa.info("Create NOAA texts")
            self.__create_file()
        except Exception as e:
            logger_lpd_noaa.error("{}".format(e))
        logger_lpd_noaa.info("exit main")
        return

    # MISC

    def get_master(self):
        """
        Get the master json that has been modified
        :return dict: self.lipd_data
        """
        return self.lipd_data

    def get_noaa_texts(self):
        """
        Retrieve the set of noaa file representations

        :return dict noaa_file_output:
        """
        return self.noaa_file_output


    # def __check_time_unit(self):
    #     """
    #     If earliest_year, and most_recent_year exist, and time_unit does NOT exist, then default time_unit = "AD"
    #     :return none:
    #     """
    #     if "Earliest_Year" in self.noaa_data_sorted["Data_Collection"] and \
    #         "Most_Recent_Year" in self.noaa_data_sorted["Data_Collection"] and \
    #         "Time_Unit" not in self.noaa_data_sorted["Data_Collection"]:
    #         self.noaa_data_sorted["Data_Collection"]["Time_Unit"] = "AD"
    #     return

    def __check_time_values(self):
        """
        Rules
        1. AD or CE units: bigger number is recent, smaller number is older
        2. BP: bigger number is older, smaller number is recent.
        3. No units: If max year is 1900-2017(current), then assume AD. Else, assume BP

        :return none:
        """
        try:
            _earliest = float(self.noaa_data_sorted["Data_Collection"]["Earliest_Year"])
        except Exception:
            _earliest = ""
        try:
            _recent = float(self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"])
        except Exception:
            _recent = ""
        try:
            _unit = self.noaa_data_sorted["Data_Collection"]["Time_Unit"]
        except Exception:
            _unit = ""

        try:
            if not _unit and not _recent and not _earliest:
                self.noaa_data_sorted["Data_Collection"]["Time_Unit"] = ""
                self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"] = ""
                self.noaa_data_sorted["Data_Collection"]["Earliest_Year"] = ""

            if _recent and _earliest:
                if not _unit:
                    # If the max value is between 1900 - 2017 (current), then assume "AD"
                    _max = max([_earliest, _recent])
                    _min = min([_earliest, _recent])
                    if _max >= 1900 and _max <= 2018:
                        self.noaa_data_sorted["Data_Collection"]["Time_Unit"] = "AD"
                        self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"] = str(_max)
                        self.noaa_data_sorted["Data_Collection"]["Earliest_Year"] = str(_min)
                    # Else, assume it's BP
                    else:
                        # Units don't exist, assume BP
                        self.noaa_data_sorted["Data_Collection"]["Time_Unit"] = "BP"
                        self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"] = str(_min)
                        self.noaa_data_sorted["Data_Collection"]["Earliest_Year"] = str(_max)
                else:
                    # Units exist
                    if _unit.lower() in ["ad", "ce"]:
                        if _earliest > _recent:
                            self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"] = str(_earliest)
                            self.noaa_data_sorted["Data_Collection"]["Earliest_Year"] = str(_recent)
                    else:
                        if _recent > _earliest:
                            self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"] = str(_earliest)
                            self.noaa_data_sorted["Data_Collection"]["Earliest_Year"] = str(_recent)
        except Exception as e:
            logger_lpd_noaa.error("check_time_values: {}".format(e))


        return

    @staticmethod
    def __convert_keys_2(header, d):
        """
        Convert lpd to noaa keys for this one section
        :param str header: Section header
        :param dict d: Metadata
        :return dict:  Metadata w/ converted keys
        """
        d_out = {}
        try:
            for k, v in d.items():
                try:
                    noaa_key = LIPD_NOAA_MAP_BY_SECTION[header][k]
                    d_out[noaa_key] = v
                except Exception:
                    print("lpd_noaa: convert_keys_section: ran into an error converting {}".format(k))
        except KeyError:
            print("lpd_noaa: convert_keys_section: KeyError: header key {} is not in NOAA_ALL_DICT".format(header))
        except AttributeError:
            print("lpd_noaa: convert_keys_section: AttributeError: metdata is wrong data type".format(header))
            return d
        return d_out

    def __convert_keys_1(self, header, d):
        """
        Loop over keys in a dictionary and replace the lipd keys with noaa keys
        :return:
        """
        d2 = {}
        try:
            for k, v in d.items():
                try:
                    d2[self.__get_noaa_key_w_context(header, k)] = v
                except KeyError:
                    pass
        except Exception:
            return d
        return d2

    @staticmethod
    def __create_blanks(section_name, d):
        """
        All keys need to be written to the output, with or without a value. Furthermore, only keys that have values
        exist at this point. We need to manually insert the other keys with a blank value. Loop through the global list
        to see what's missing in our dict.
        :param str section_name: Retrieve data from global dict for this section
        :return none:
        """
        try:
            for key in NOAA_KEYS_BY_SECTION[section_name]:
                if key not in d:
                    # Key not in our dict. Create the blank entry.
                    d[key] = ""
        except Exception:
            print("lpd_noaa: create_blanks: must section: {}, key".format(section_name, key))
        return d

    @staticmethod
    def __flatten_col(d):
        """
        Flatten column so climateInterpretation and calibration are not nested.
        :param d:
        :return:
        """

        try:
            for entry in ["climateInterpretation", "calibration"]:
                if entry in d:
                    for k, v in d[entry].items():
                        d[k] = v
                    del d[entry]
        except AttributeError:
            pass

        return d

    def __generate_study_name(self):
        """
        When a study name is not given, generate one with the format of " author - site name - year "
        :return str study_name: generated study name
        """
        study_name = ""
        _exist = False
        try:
            if self.noaa_data_sorted["Top"]["Study_Name"]:
                _exist = True
        except KeyError:
            pass

        if not _exist:
            try:
                _site = self.noaa_data_sorted["Site_Information"]["properties"]["siteName"]
                _year = self.noaa_data_sorted["Publication"][0]["pubYear"]
                _author = self.noaa_data_sorted["Publication"][0]["author"]
                _author = self.__get_author_last_name(_author)
                study_name = "{}.{}.{}".format(_author, _site,  _year)
                study_name = study_name.replace(" ", "_").replace(",", "_")
            except (KeyError, Exception):
                pass
            self.noaa_data_sorted["Top"]["Study_Name"] = study_name
            self.noaa_data_sorted["Title"]["Study_Name"] = study_name
        return

    def __is_notes(self):
        """
        Determine if notes already exist or not.
        :return bool:
        """
        try:
            if "Description" in self.noaa_data_sorted["Description_Notes_and_Keywords"]:
                if self.noaa_data_sorted["Description_Notes_and_Keywords"]["Description"]:
                    # Found data. Leave as-is
                    return False
        except KeyError:
            pass
        # Error or no data found. Try to make notes using QCnotes
        return True

    def __lists_to_str(self):
        """
        There are some data lists that we collected across the dataset that need to be concatenated into a single
        string before writing to the text file.
        :return none:
        """
        # ["data_type", "sensor_genus", "sensor_species", "investigator"]
        if self.lsts_tmp["Data_Type"]:
            self.noaa_data_sorted["Top"]["Data_Type"] = ",".join(self.lsts_tmp["Data_Type"])
        if self.lsts_tmp["species"]:
            self.noaa_data_sorted["Species"]["Species_Name"] = ",".join(self.lsts_tmp["species"])
        if self.lsts_tmp["genus"]:
            self.noaa_data_sorted["Species"]["Species_Code"] = ",".join(self.lsts_tmp["genus"])
        if self.lsts_tmp["qc"]:
            if self.__is_notes():
                self.noaa_data_sorted["Description_Notes_and_Keywords"]["Description"] = ";".join(self.lsts_tmp["qc"])
        return

    def __parse_dois(self, x):
        """
        Parse the Dataset_DOI field. Could be one DOI string, or a list of DOIs
        :param any x: Str or List of DOI ids
        :return none: list is set to self
        """
        # datasetDOI is a string. parse, validate and return a list of DOIs
        if isinstance(x, str):
            # regex cleans string, and returns a list with 1 entry for each regex doi match
            m = clean_doi(x)
            # make sure m is not an empty list
            if m:
                # set list directly into self
                self.doi = m

        # datasetDOI is a list. use regex to validate each doi entry.
        elif isinstance(x, list):
            for entry in x:
                # regex cleans string, and returns a list with 1 entry for each regex doi match
                m = clean_doi(entry)
                # make sure m is not an empty list
                if m:
                    # combine lists with existing self list
                    self.doi += m
        return

    @staticmethod
    def __split_path(string):
        """
        Used in the path_context function. Split the full path into a list of steps
        :param str string: Path string ("geo-elevation-height")
        :return list out: Path as a list of strings. One entry per path step.(["geo", "elevation", "height"])
        """
        out = []
        position = string.find(':')
        if position != -1:
            # A position of 0+ means that ":" was found in the string
            key = string[:position]
            val = string[position+1:]
            out.append(key)
            out.append(val)
            if ('-' in key) and ('Funding' not in key) and ('Grant' not in key):
                out = key.split('-')
                out.append(val)
        return out

    @staticmethod
    def _values_exist(table):
        """
        Check that values exist in this table, and we can write out data columns
        :param dict table: Table data
        :return bool: Values exist or not exist
        """
        try:
            for var, data in table["columns"].items():
                if "values" in data:
                    return True
        except KeyError as e:
            print("values_exist: KeyError: {}".format(e))
        except Exception as e:
            print("values_exist: Excpetion: {}".format(e))
        return False

    # REORGANIZE

    def __reorganize(self):
        """
        Reorganize the keys into their proper section order for the NOAA output file
        DO NOT parse data tables (paleoData or chronData). We will do those separately.
        :param str key:
        :param any value:
        :return none:
        """
        logger_lpd_noaa.info("Enter: reorganize")
        try:
            # NOAA files are organized in sections differently than NOAA. try to translate these sections.
            for key, value in self.lipd_data.items():
                # if this key has a noaa match, it'll be returned. otherwise, empty string for no match
                noaa_key = self.__get_noaa_key(key)
                # check if this lipd key is in the NOAA_KEYS conversion dictionary.
                # if it's not, then stash it in our ignore list.
                if key not in LIPD_NOAA_MAP_FLAT:
                    self.noaa_data_sorted["Ignore"][noaa_key] = value
                elif key == "notes":
                    self.noaa_data_sorted["Description_Notes_and_Keywords"]["Description"] = value
                # studyName is placed two times in file. Line #1, and under the 'title' section
                elif noaa_key == "Study_Name":
                    # study name gets put in two locations
                    self.noaa_data_sorted["Top"][noaa_key] = value
                    self.noaa_data_sorted["Title"][noaa_key] = value
                # put NOAADataType (archiveType) in self, because we'll reuse it later for the 9-part-variables as well
                elif noaa_key == "Data_Type":
                    self.lsts_tmp["Data_Type"].append(value)
                # Dataset_DOI is a repeatable element. the key could be a single DOI, or a list of DOIs.
                elif noaa_key == "Dataset_DOI":
                    self.__parse_dois(value)

                # all other keys. determine which noaa section they belong in.
                else:
                    # noaa keys are sorted by section.
                    for header, content in NOAA_KEYS_BY_SECTION.items():
                        try:
                            # if our key is a noaa header key, then that means it's the ONLY key in the section.
                            # set value directly
                            if noaa_key == header:
                                self.noaa_data_sorted[header] = value
                            # all other cases, the key is part of the section
                            elif noaa_key in content:
                                self.noaa_data_sorted[header][noaa_key] = value
                        except Exception as e:
                            # this shouldn't ever really happen, but just in case
                            logger_lpd_noaa.error("reorganize: {}, {}".format(noaa_key, e))
        except Exception as e:
            logger_lpd_noaa.error("reorganize: {}, {}".format(key, e))
            logger_lpd_noaa.error("reorganize: {}".format(value))
        logger_lpd_noaa.info("exit reorganize")
        return

    def __reorganize_author(self):
        """
        LiPD delimits author names by "and". Noaa wants them to be semi-colon delimited.
        :return none:
        """
        try:
            for idx, pub in enumerate(self.noaa_data_sorted["Publication"]):
                if "author" in pub:
                    _str = pub["author"]
                    if " and " in _str:
                        self.noaa_data_sorted["Publication"][idx]["author"] = _str.replace(" and ", "; ")
                    if ";" in _str:
                        self.noaa_data_sorted["Publication"][idx]["author"] = _str.replace(";", "; ")

        except Exception:
            pass
        return

    def __reorganize_coordinates(self):
        """
        GEOJSON FORMAT :  [ LONGITUDE, LATITUDE, ELEVATION]
        Reorganize coordinates based on how many values are available.
        :return:
        """
        try:
            l = self.noaa_data_sorted["Site_Information"]['geometry']['coordinates']
            locations = ["Northernmost_Latitude", "Southernmost_Latitude", "Easternmost_Longitude",
                         "Westernmost_Longitude", "Elevation"]
            print("coordinates: {} coordinates found".format(len(l)))

            # Amount of coordinates in the list
            _len_coords = len(l)

            # Odd number of coordinates. Elevation value exists
            if _len_coords % 2 == 1:
                # Store the elevation, which is always the last value in the list
                self.noaa_geo["Elevation"] = l[-1]
                # If elevation, then subtract one from the length
                _len_coords -= 1

            # Start compiling the lat lon coordinates

            # 0 coordinate values. fill in locations with empty values
            if _len_coords == 0:
                for location in locations:
                    self.noaa_geo[location] = ' '
            # 2 coordinates values. duplicate to fill 4 location slots.
            elif _len_coords == 2:
                self.noaa_geo[locations[0]] = l[1]
                self.noaa_geo[locations[1]] = l[1]
                self.noaa_geo[locations[2]] = l[0]
                self.noaa_geo[locations[3]] = l[0]

            # 4 coordinate values. put each in its correct location slot.
            elif _len_coords == 4:
                for index, location in enumerate(locations):
                    self.noaa_geo[locations[index]] = l[index]
            else:
                print("coordinates: too many coordinates given")
        except KeyError as e:
            logger_lpd_noaa.error("coordinates: no coordinate information: {}".format(e))
        except Exception as e:
            logger_lpd_noaa.error("coordinates: unknown exception".format(e))

        return

    def __reorganize_funding(self):
        """
        Funding gets added to noaa_data_sorted with LiPD keys. Change those keys to NOAA
        :return none:
        """
        _map = {"agency": "Funding_Agency_Name", "grant": "Grant"}
        try:
            _l = []
            for item in self.noaa_data_sorted["Funding_Agency"]:
                _tmp = {}
                for lpd_name, noaa_name in _map.items():
                    val = ""
                    if lpd_name in item:
                        val = item[lpd_name]
                    _tmp[noaa_name] = val
                _l.append(_tmp)
            self.noaa_data_sorted["Funding_Agency"] = _l
        except Exception:
            pass
        return

    def __reorganize_geo(self):
        """
        Concat geo value and units, and reorganize the rest
        References geo data from self.noaa_data_sorted
        Places new data into self.noaa_geo temporarily, and then back into self.noaa_data_sorted.
        :return:
        """
        print("enter reorganize_geo")

        try:
            # Geo -> Properties
            for k, v in self.noaa_data_sorted["Site_Information"]['properties'].items():
                noaa_key = self.__get_noaa_key(k)
                self.noaa_geo[noaa_key] = v
        except KeyError:
            print("reorganize_geo: KeyError: geo properties")
        try:
            # Geo -> Geometry
            self.__reorganize_coordinates()
        except Exception:
            logger_lpd_noaa.error("reorganize_geo: missing required data: coordinates")

        # put the temporarily organized data into the self.noaa_data_sorted
        self.noaa_data_sorted["Site_Information"] = self.noaa_geo
        return

    def __reorganize_sensor(self):
        """
        We have raw sensorGenus, and sensorSpecies in self, now clean and sort
        :return none:
        """
        _code = []
        _name = []

        # Check if any of the sensor data is misplaced, and create corrected lists.
        if self.lsts_tmp["genus"]:
            for name in self.lsts_tmp["genus"]:
                if len(name) == 4 and name.isupper():
                    _code.append(name)
                else:
                    _name.append(name)

        if self.lsts_tmp["species"]:
            for name in self.lsts_tmp["species"]:
                if len(name) != 4 and not name.isupper():
                    _name.append(name)
                else:
                    _code.append(name)

        # Set the strings into the noaa data sorted
        self.lsts_tmp["species"] = _name
        self.lsts_tmp["genus"] = _code

        return

    # PUT

    def __put_dois(self):
        """
        Put the DOIs into their proper sections
        :return:
        """
        # todo: fn may not be necessary. I image pub DOIs and dataset_DOIs are mutually exclusive.
        # todo: therefore, collecting DOIs from pubs to put into the dataset_DOI is probably wrong.
        pass

    @staticmethod
    def __put_names_on_csv_cols(names, cols):
        """
        Put the variableNames with the corresponding column data.
        :param list names: variableNames
        :param list cols: List of Lists of column data
        :return dict:
        """
        _combined = {}
        for idx, name in enumerate(names):
            # Use the variableName, and the column data from the same index
            _combined[name] = cols[idx]



        return _combined

    @staticmethod
    def __put_year_col_first(d):
        """
        Always write year column first. Reorder dictionary so that year is first
        :param dict d: data
        :return dict: Reordered data
        """
        if "year" in d:
            D = OrderedDict()
            # store the year column first
            D["year"] = d["year"]
            for k,v in d.items():
                if k != "year":
                    # store the other columns
                    D[k] = v
            return D
        else:
            # year is not found, return data as-is
            return d

    # REMOVE

    def __rm_duplicate_citation_2(self, key):
        """
        Remove exact duplicate entries for abstract or citation fields. Check all publication entries.
        :return:
        """
        citations = []
        # Before writing, remove any duplicate "Full_Citations" that are found
        for idx, pub in enumerate(self.noaa_data_sorted["Publication"]):
            try:
                citations.append(pub[key])
            except KeyError:
                # Key was not found. Enter a blank entry in citations list
                citations.append("")

        # Create a backwards dictionary, that lists which indexes are duplicates
        d = defaultdict(list)
        for i, item in enumerate(citations):
            d[item].append(i)
        d = {k: v for k, v in d.items() if len(v) > 1}
        # Duplicates indexes are listed like so:
        # {"full citation info here": [0, 2, 5]}

        # Loop over duplicate indexes
        for citation, idxs in d.items():
            # do not process any duplicate entries that were initially blanks.
            if citation:
                # Loop for [1:], since we want to keep the first citation and remove the rest.
                for idx in idxs[1:]:
                    # Set citation to blank
                    self.noaa_data_sorted["Publication"][idx][key] = ""

    def __rm_duplicate_citation_1(self):
        """
        If there are multiple publications, and there are two duplicate "Full_Citation" entries, then keep only one
        of the Full_Citation entries and remove the others.
        :return none:
        """
        # Loop through and collect all citations
        # Then, see if any citations are duplicates (excluding blank entry)
        # Keep the first entry,
        # then set subsequent duplicate citations to blank for corresponding publications indexes
        for key in ["abstract", "citation"]:
            self.__rm_duplicate_citation_2(key)

        return

    @staticmethod
    def __rm_names_on_csv_cols(d):
        """
        Remove the variableNames from the columns.
        :param dict d: Named csv data
        :return list list: One list for names, one list for column data
        """
        _names = []
        _data = []
        for name, data in d.items():
            _names.append(name)
            _data.append(data)
        return _names, _data

    # GET

    @staticmethod
    def __get_author_last_name(author):
        """
        Take a string of author(s), get the first author name, then get the first authors last name only.

        :param str author: Author(s)
        :return str _author: First author's last name
        """
        _author = ""
        if isinstance(author, str):
            try:
                # example:   'Ahmed, Moinuddin and Anchukaitis, Kevin J and ...'
                # If this is a list of authors, try to split it and just get the first author.
                if " and " in author:
                    _author = author.split(" and ")[0]
                # 'Ahmed, Moinuddin; Anchukaitis, Kevin J;  ...'
                elif ";" in author:
                    _author = author.split(";")[0]
            except Exception:
                _author = ""
            try:
                # example :  'Ahmed Moinuddin,  Anchukaitis Kevin J,  ...'
                _author = author.replace(" ", "")
                if "," in author:
                    _author = author.split(",")[0]
            except Exception:
                _author = ""
        elif isinstance(author, list):
            try:
                # example:  [{'name': 'Ahmed, Moinuddin'}, {'name': 'Anchukaitis, Kevin J.'}, ..]
                # just get the last name of the first author. this could get too long.
                _author = author[0]["name"].split(",")[0]
            except Exception as e:
                _author = ""

        return _author

    def __create_author_investigator_str(self):
        """
        When investigators is empty, try to get authors from the first publication instead.
        :return str author: Author names
        """
        _author = ""
        try:
            for pub in self.noaa_data_sorted["Publication"]:
                if "author" in pub:
                    if pub["author"]:
                        _author_src = pub["author"]
                        if isinstance(_author_src, str):
                            try:
                                if " and " in _author_src:
                                    _author = _author_src.replace(" and ", "; ")
                                elif ";" in _author_src:
                                    # If there is a semi-colon, add a space after it, just in case it didn't have one
                                    _author = _author_src.replace(";", "; ")
                                break
                            except Exception as e:
                                _author = ""
                        elif isinstance(_author_src, list):
                            try:
                                for _entry in _author_src:
                                    _author += _entry["name"].split(",")[0] + ", "
                            except Exception as e:
                                _author = ""
        except Exception:
            _author = ""
        return _author

    @staticmethod
    def __get_pub_type(pub):
        """
        Check this publication to see if it is a data citation.
        :param dict pub: Publication
        :return bool: True, if data citation, False, if not data citation.
        """
        try:
            # The given publication IS a data citation.
            if pub["type"] == "dataCitation":
                return True
        except KeyError:
            # Publication is missing "type" field. Assume that publication IS NOT a data citation.
            pass
        return False

    @staticmethod
    def __get_filename(table):
        """
        Get filename from a data table
        :param dict table:
        :return str:
        """
        try:
            filename = table['filename']
        except KeyError as e:
            filename = ""
            print("get_filename: KeyError: Table missing filename, {}".format(e))
        except TypeError:
            try:
                filename = table[0]["filename"]
            except Exception as e:
                filename = ""
                logger_lpd_noaa.error("get_filename: Unable to get filename from table, {}".format(e))
        return filename

    @staticmethod
    def __get_doi(pub):
        """
        Get DOI from this ONE publication entry.
        :param dict pub: Single publication entry
        :return:
        """
        doi = ""
        # Doi location: d["pub"][idx]["identifier"][0]["id"]
        #  04.19.19 - doi moved locations to -> d["pub"][idx]["doi"]
        try:
            # doi = pub["DOI"][0]["id"]
            doi = pub["doi"]
            doi = clean_doi(doi)
        except KeyError:
            print("get_dois: KeyError: missing a doi key")
        except Exception as e:
            logger_lpd_noaa.error("get_dois: {}".format(e))

        # if we received a doi that's a list, we want to concat into a single string
        if isinstance(doi, list):
            if len(doi) == 1:
                doi = doi[0]
            else:
                ", ".join(doi)
        return doi

    @staticmethod
    def __get_mv(d):
        """
        Get missing value from root of data table dictionary.
        :param dict d: Data table
        :return str: Missing value or empty str.
        """
        if 'missingValue' in d:
            return d['missingValue']
        return ''

    @staticmethod
    def __get_noaa_key(lipd_key):
        """
        Switch a LiPD key into its NOAA equivalent
        :param str lipd_key: LiPD Key
        :return str: NOAA key
        """
        try:
            noaa_key = LIPD_NOAA_MAP_FLAT[lipd_key]
        except KeyError:
            # logger_lpd_noaa.warning("lpd_noaa: map_key: unable to find noaa mapping for lipd key: {}".format(lipd_key))
            return lipd_key
        return noaa_key

    @staticmethod
    def __get_noaa_key_w_context(header, lipd_key):
        """
        Use the section header and key to get the proper noaa key.
        :param str header:
        :param str lipd_key:
        :return:
        """
        try:
            noaa_key = LIPD_NOAA_MAP_BY_SECTION[header][lipd_key]
        except KeyError:
            return lipd_key
        return noaa_key

    def __get_overall_data(self, x):
        """
        (recursive) Collect all "sensorGenus" and "sensorSpecies" fields, set data to self
        :param any x: Any data type
        :return none:
        """
        if isinstance(x, dict):
            if "sensorGenus" in x:
                if x["sensorGenus"] and x["sensorGenus"] not in self.lsts_tmp["genus"]:
                    self.lsts_tmp["genus"].append(x["sensorGenus"])
            if "sensorSpecies" in x:
                if x["sensorSpecies"] and x["sensorSpecies"] not in self.lsts_tmp["species"]:
                    self.lsts_tmp["species"].append(x["sensorSpecies"])
            if "NOAAdataType" in x:
                if x["NOAAdataType"] and x["NOAAdataType"] not in self.lsts_tmp["Data_Type"]:
                    self.lsts_tmp["Data_Type"].append(x["NOAAdataType"])
            if "QCnotes" in x:
                if x["QCnotes"] and x["QCnotes"] not in self.lsts_tmp["qc"]:
                    self.lsts_tmp["qc"].append(x["QCnotes"])

            for k, v in x.items():
                if isinstance(v, dict):
                    self.__get_overall_data(v)
                elif isinstance(v, list):
                    self.__get_overall_data(v)
        elif isinstance(x, list):
            for i in x:
                self.__get_overall_data(i)

        return x

    def __get_max_min_time_1(self, table):
        """
        Search for either Age or Year to calculate the max, min, and time unit for this table/file.
        Preference: Look for Age first, and then Year second (if needed)

        :param dict table: Table data
        :return dict: Max, min, and time unit
        """
        try:
            # find the values and units we need to calculate
            _vals, _units, _min, _max = self.__get_max_min_time_2(table, ["age", "yearbp", "yrbp"], True)

            if not _vals and not _units and not _min and not _max:
                _vals, _units, _min, _max = self.__get_max_min_time_2(table, ["year", "yr"], True)

            if not _vals and not _units and not _min and not _max:
                _vals, _units, _min, _max = self.__get_max_min_time_2(table, ["age", "yearbp", "yrbp"], False)

            if not _vals and not _units and not _min and not _max:
                _vals, _units, _min, _max = self.__get_max_min_time_2(table, ["year", "yr"], False)

            # now put this data into the noaa data sorted self for writing to file.
            # year farthest in the past

            if not _min and not _max:
                _vals2 = []
                for i in _vals:
                    try:
                        _vals2.append(float(i))
                    except ValueError:
                        pass
                _max = max(_vals2)
                _min = min(_vals2)

            if _min or _min in [0, 0.0]:
                self.noaa_data_sorted["Data_Collection"]["Earliest_Year"] = str(_min)
            if _max or _max in [0, 0.0]:
                self.noaa_data_sorted["Data_Collection"]["Most_Recent_Year"] = str(_max)
            # AD or... yrs BP?
            self.noaa_data_sorted["Data_Collection"]["Time_Unit"] = _units

        except Exception as e:
            logger_lpd_noaa.error("get_max_min_time_1: {}".format(e))

        return

    @staticmethod
    def __get_max_min_time_2(table, terms, exact):
        """
        Search for either Age or Year to calculate the max, min, and time unit for this table/file.
        Preference: Look for Age first, and then Year second (if needed)
        :param dict table: Table data
        :param list terms: age, yearbp, yrbp,  or year, yr
        :param bool exact: Look for exact key match, or no
        :return list: Year / age values
        :return str: units
        :return float: Min value
        :return float: Max value
        """
        _vals = []
        _unit = ""
        _max = ""
        _min = ""
        try:
            for k, v in table["columns"].items():
                if exact:
                    if k.lower() in terms:
                        try:
                            _vals = v["values"]
                            _unit = v["units"]
                            if "hasMinValue" in v and "hasMaxValue" in v:
                                _max = v["hasMaxValue"]
                                _min = v["hasMinValue"]
                            break
                        except KeyError:
                            pass
                elif not exact:
                    for term in terms:
                        if term in k:
                            try:
                                _vals = v["values"]
                                _unit = v["units"]
                                if "hasMinValue" in v and "hasMaxValue" in v:
                                    _max = v["hasMaxValue"]
                                    _min = v["hasMinValue"]
                                break
                            except KeyError:
                                pass

        except Exception as e:
            logger_lpd_noaa.error("get_max_min_time_2: {}".format(e))

        return _vals, _unit, _min, _max

    def __get_data_citation(self, l):
        """
        If originalDataURL / investigators not in root data, check for a dataCitation pub entry.
        :return:
        """
        # loop once for each publication entry
        for pub in l:
            try:
                # at the moment, these are the only keys of interest inside of dataCitation. Check each.
                for key in ["url", "investigators"]:
                    if pub["type"] == "dataCitation" and key in pub:
                        noaa_key = self.__get_noaa_key(key)
                        self.data_citation[noaa_key] = pub[key]
            except KeyError:
                # no "type" key in pub
                logger_lpd_noaa.error("get_data_citation: KeyError: pub is missing 'type'")
        return

    def __get_output_filenames(self):
        """
        Set up the output filenames. If more than one file is being written out, we need appended filenames.
        :return:
        """
        try:
            # if there is only one file, use the normal filename with nothing appended.
            if len(self.noaa_data_sorted["Data"]) == 1:
                self.output_filenames.append(self.filename_txt)

            # if there are multiple files that need to be written out, (multiple data table pairs), then append numbers
            elif len(self.noaa_data_sorted["Data"]) > 1:
                for i in range(0, self.output_file_ct):
                    tmp_name = "{}-{}.txt".format(self.dsn, i+1)
                    self.output_filenames.append(tmp_name)
        except Exception as e:
            logger_lpd_noaa.error("get_output_filenames: {}".format(e))
        return

    def __get_table_pairs(self):
        """
        Use the tables in self.paleos and self.chrons (sorted by idx) to put in self.noaa_data_sorted.
        :return:
        """
        try:
            for _idx, _p in enumerate(self.data_paleos):
                _c = {}
                try:
                    _c = self.data_chrons[_idx]
                except IndexError:
                    pass
                # create entry in self object collection of data tables
                self.noaa_data_sorted["Data"].append({"paleo": _p, "chron": _c})
        except KeyError:
            logger_lpd_noaa.error("get_meas_table: 0 paleo data tables")

        self.output_file_ct = len(self.noaa_data_sorted["Data"])
        return

    def __get_table_count(self):
        """
        Check how many tables are in the json
        :return int:
        """
        _count = 0
        try:
            keys = ["paleo", "paleoData", "measurementTable"]
            # get the count for how many tables we have. so we know to make appended filenames or not.
            for pd_name, pd_data in self.lipd_data[keys[1]].items():
                for table_name, table_data in pd_data[keys[2]].items():
                    _count += 1

        except Exception as e:
            logger_lpd_noaa.error("get_table_count: {}".format(e))

        if _count > 1:
            self.append_filenames = True

        return

    def __put_tables_in_self(self, keys):
        """
        Metadata is sorted-by-name. Use this to put the table data into the object self.
        :param list keys: Paleo or Chron keys
        :return none:
        """
        _idx = 1
        try:
            # start adding the filenames to the JSON and adding the tables to the self tables
            for pd_name, pd_data in self.lipd_data[keys[1]].items():
                for table_name, table_data in pd_data[keys[2]].items():
                    if self.append_filenames:
                        # _url = "https://www1.ncdc.noaa.gov/pub/data/paleo/pages2k/{}-{}/data-version-{}/{}-{}.txt".format(self.project, self.current_yr, self.version, self.dsn, _idx)
                        _url = ""
                        _url = re.sub("['{}@!$&*+,;?%#~`\[\]=]", "", _url)
                        self.lipd_data[keys[1]][pd_name][keys[2]][table_name]["WDSPaleoUrl"] = _url
                        table_data["WDSPaleoUrl"] = _url
                        if keys[0] == "paleo":
                            self.data_paleos.append(table_data)
                        else:
                            self.data_chrons.append(table_data)
                    else:
                        _url = re.sub("['{}@!$&*+,;?%#~`\[\]=]", "", self.noaa_url)
                        self.lipd_data[keys[1]][pd_name][keys[2]][table_name]["WDSPaleoUrl"] = _url
                        table_data["WDSPaleoUrl"] = _url
                        if keys[0] == "paleo":
                            self.data_paleos.append(table_data)
                        else:
                            self.data_chrons.append(table_data)
                    _idx += 1
        except Exception as e:
            logger_lpd_noaa.error("put_tables_in_self: {}".format(e))

        return

    def get_wdc_paleo_url(self):
        """
        When a NOAA file is created, it creates a URL link to where the dataset will be hosted in NOAA's archive
        Retrieve and add this link to the original LiPD file, so we can trace the dataset to NOAA.
        :return str:
        """
        return self.noaa_url

    # CREATE FILE REPRESENTATIONS

    def __create_file(self):
        """
        Open text file. Write one section at a time. Close text file. Move completed file to dir_root/noaa/
        :return none:
        """
        logger_lpd_noaa.info("enter create_file")
        self.__get_output_filenames()
        logger_lpd_noaa.info(self.output_filenames)
        # print(self.noaa_data_sorted)

        for idx, filename in enumerate(self.output_filenames):
            try:
                # self.noaa_txt = open(os.path.join(self.path, filename), "w+")
                # self.noaa_file_output[filename] = ""
                # self.noaa_txt = self.noaa_file_output[filename]
                self.noaa_txt = ""
                logger_lpd_noaa.info("creating: {}".format(filename))
                logger_lpd_noaa.info("write_file: opened output txt file")
            except Exception as e:
                logger_lpd_noaa.error("write_file: failed to open output txt file, {}".format(e))
                return

            self.__get_max_min_time_1(self.noaa_data_sorted["Data"][idx]["paleo"])
            self.__check_time_values()

            self.__write_top(filename)
            self.__write_generic('Contribution_Date')
            self.__write_generic('File_Last_Modified_Date')
            self.__write_generic('Title')
            self.__write_generic('Investigators')
            self.__write_generic('Description_Notes_and_Keywords')
            self.__write_pub()
            self.__write_funding()
            self.__write_geo()
            self.__write_generic('Data_Collection')
            self.__write_generic('Species')
            self.__write_data(idx)

            self.noaa_file_output[filename] = self.noaa_txt
            # logger_lpd_noaa.info("closed output text file")
            # reset the max min time unit to none
            self.max_min_time = {"min": "", "max": "", "time": ""}
            # shutil.copy(os.path.join(os.getcwd(), filename), self.dir_root)
            logger_lpd_noaa.info("exit create_file")
        # print(self.noaa_file_output)
        return

    def __write_top(self, filename):
        """
        Write the top section of the txt file.
        :param int section_num: Section number
        :return none:
        """
        logger_lpd_noaa.info("writing section: {}".format("top"))
        self.__create_blanks("Top", self.noaa_data_sorted["Top"])

        # Start writing the NOAA file section by section, starting at the very top of the template.
        self.noaa_txt += "# {}".format(self.noaa_data_sorted["Top"]['Study_Name'])
        self.__write_template_top()
        for i in self.noaa_data_sorted["Top"]["Online_Resource"]:
            self.__write_k_v("Online_Resource ", i["onlineResource"], top=True)
            self.__write_k_v("Description ", i["description"], indent=True)

        # We don't know what the full online resource path will be yet, so leave the base path only
        # self.__write_k_v("Online_Resource", " https://www1.ncdc.noaa.gov/pub/data/paleo/pages2k/{}-{}/data-version-{}/{}".format(self.project, self.current_yr, self.version, filename), top=True)
        # self.__write_k_v("Description", " This file.  NOAA WDS Paleo formatted metadata and data for version {} of this dataset.".format(self.version), indent=True)
        # self.__write_k_v("Online_Resource", " https://www1.ncdc.noaa.gov/pub/data/paleo/pages2k/{}-{}/data-version-{}/{}".format(self.project, self.current_yr, self.version, self.filename_lpd), top=True)
        # self.__write_k_v("Description", " Linked Paleo Data (LiPD) formatted file containing the same metadata and data as this file, for version {} of this dataset.".format(self.version), indent=True)
        self.__write_k_v("Original_Source_URL", self.noaa_data_sorted["Top"]['Original_Source_URL'], top=True)
        self.__write_k_v("Original_Source_URL_Description", self.noaa_data_sorted["Top"]["Original_Source_URL_Description"], top=False)
        self.noaa_txt += "\n# Description/Documentation lines begin with #\n# Data lines have no #\n#"
        self.__write_k_v("Data_Type", self.noaa_data_sorted["Top"]['Data_Type'])
        self.__write_k_v("Parameter_Keywords", self.noaa_data_sorted["Top"]['Parameter_Keywords'])

        # get the doi from the pub section of self.steps_dict[6]
        # doi = self.__get_doi()
        self.__write_k_v("Dataset_DOI",  ', '.join(self.doi), False, True, False, False)
        # self.__write_k_v("Parameter_Keywords", self.steps_dict[section_num]['parameterKeywords'])
        self.__write_divider()
        logger_lpd_noaa.info("exit write_top")
        return

    def __write_generic(self, header, d=None):
        """
        Write a generic section to the .txt. This function is reused for multiple sections.
        :param str header: Header title for this section
        :param int section_num:
        :param dict d: Section from steps_dict
        :return none:
        """
        logger_lpd_noaa.info("writing section: {}".format(header))
        if not d:
            d = self.noaa_data_sorted[header]
        d = self.__create_blanks(header, d)
        self.__write_header_name(header)
        for key in NOAA_KEYS_BY_SECTION[header]:
            key = self.__get_noaa_key_w_context(header, key)
            # NOAA writes value and units on one line. Build the string here.
            # if key == 'coreLength':
            #     value, unit = self.__get_corelength(val)
            #     val = str(value) + " " + str(unit)
            # DOI  id is nested in "identifier" block. Retrieve it.
            if key == "DOI":
                val = self.__get_doi(d)
                # Don't write out an empty DOI list. Write out empty string instead.
                if not val:
                    val = ""
            elif key == "Investigators":
                # Investigators is a section by itself. "d" should be a direct list
                val = get_authors_as_str(d)
                # If we don't have investigator data, use the authors from the first publication entry
                if not val:
                    val = self.__create_author_investigator_str()
            # lipd uses a singular "author" key, while NOAA uses plural "authors" key.
            elif key in ["Author", "Authors"]:
                # "d" has all publication data, so only pass through the d["Authors"] piece
                val = get_authors_as_str(d[key])
            elif key == "Collection_Name":
                if not d[key]:
                    # If there is not a collection name, then use the dsn so _something_ is there.
                    val = self.dsn
            else:
                val = d[key]

            # Write the output line
            self.__write_k_v(str(self.__get_noaa_key(key)), val, indent=True)
        # Don't write a divider if there isn't a Chron section after species. It'll make a double.
        # if header == "Species" and not self.noaa_data_sorted["Species"]:
        #     return
        self.__write_divider()
        return

    def __write_pub(self):
        """
        Write pub section. There may be multiple, so write a generic section for each one.
        :return none:
        """
        try:
            self.__reorganize_author()
            # Check all publications, and remove possible duplicate Full_Citations
            self.__rm_duplicate_citation_1()
            if not self.noaa_data_sorted["Publication"]:
                self.noaa_data_sorted["Publication"].append({"pubYear": ""})
            for idx, pub in enumerate(self.noaa_data_sorted["Publication"]):
                print("publication: {}".format(idx))
                # Do not write out Data Citation publications. Check, and skip if necessary
                is_data_citation = self.__get_pub_type(pub)
                if not is_data_citation or is_data_citation and len(self.noaa_data_sorted["Publication"]) == 1:
                    pub = self.__convert_keys_1("Publication", pub)
                    self.__write_generic('Publication', pub)
        except KeyError:
            logger_lpd_noaa.error("write_pub: KeyError: pub section not found")
        except TypeError:
            logger_lpd_noaa.error("write_pub: TypeError: pub not a list type")

        return

    def __write_funding(self):
        """
        Write funding section. There are likely multiple entries.
        :param dict d:
        :return none:
        """
        self.__reorganize_funding()
        # if funding is empty, insert a blank entry so that it'll still write the empty section on the template.
        if not self.noaa_data_sorted["Funding_Agency"]:
            self.noaa_data_sorted["Funding_Agency"].append({"grant": "", "agency": ""})
        for idx, entry in enumerate(self.noaa_data_sorted["Funding_Agency"]):
            print("funding: {}".format(idx))
            self.__write_generic('Funding_Agency', entry)
        return

    def __write_geo(self):
        """
        Write geo section. Organize before writing to file.
        :param dict d:
        :return none:
        """
        self.__reorganize_geo()
        self.__write_generic('Site_Information')
        return

    def __write_data(self, idx):
        """
        Write out the measurement tables found in paleoData and chronData
        :return:
        """
        pair = self.noaa_data_sorted["Data"][idx]
        # Run once for each pair (paleo+chron) of tables that was gathered earlier.
        # for idx, pair in enumerate(self.noaa_data_sorted["Data"]):
        lst_pc = ["chron", "paleo"]
        # loop once for paleo, once for chron
        for pc in lst_pc:
            # safeguard in case the table is an empty set.
            table = pair[pc]
            if pc == "paleo":
                self.__write_variables_1(table)
                self.__write_divider()
                self.__write_columns(pc, table)
            elif pc == "chron":
                # self.__write_variables_1(table)
                self.__write_columns(pc, table)
                self.__write_divider()
        return

    def __write_variables_1(self, table):
        """
        Retrieve variables from data table(s) and write to Variables section of txt file.
        :param dict table: Paleodata
        :return none:
        """
        print("writing section: {}".format("Variables"))

        # Write the template lines first
        self.__write_template_variable()

        try:
            self.noaa_txt += '#'

            # Special NOAA Request: Write the "year" column first always, if available
            # write year data first, when available
            for name, data in table["columns"].items():
                if name == "year":
                    # write first line in variables section here
                    self.__write_variables_2(data)
                    # leave the loop, because this is all we needed to accomplish
                    break

            # all other cases
            for name, data in table["columns"].items():
                # we already wrote out the year column, so don't duplicate.
                if name != "year":
                    self.__write_variables_2(data)

        except KeyError as e:
            print("write_variables: KeyError: {} not found".format(e))
        return

    def __write_variables_2(self, col):
        """
        Use one column of data, to write one line of data in the variables section.
        :return none:
        """
        col = self.__convert_keys_1("Variables", col)
        # Write one line for each column. One line has all metadata for one column.
        for entry in NOAA_KEYS_BY_SECTION["Variables"]:
            # May need a better way of handling this in the future. Need a strict list for this section.
            try:
                # First entry: Add extra hash and tab
                if entry == 'shortname':
                    # DEPRECATED: Fixed spacing for variable names.
                    # self.noaa_txt.write('{:<20}'.format('#' + str(col[entry])))
                    # Fluid spacing for variable names. Spacing dependent on length of variable names.
                    self.noaa_txt += '{}\t'.format('#' + str(col[entry]))
                # Last entry: No space or comma
                elif entry == "additional":
                    e = " "
                    for item in ["notes", "uncertainty"]:
                        try:
                            if col[item]:
                                e += str(col[item]).replace(",", ";") + "; "
                        except KeyError:
                            pass
                    self.noaa_txt += '{} '.format(e)

                # elif entry == 'notes':
                #     self.noaa_txt.write('{} '.format(str(col[entry])))
                else:
                    # logger_lpd_noaa.info("write variables 2 key: {}".format(entry))
                    # This is for any entry that is not first or last in the line ordering
                    # Account for nested entries.
                    # if entry == "uncertainty":
                    #     try:
                    #         e = str(col["calibration"][entry])
                    #     except KeyError:
                    #         e = ""
                    if entry == "seasonality":
                        try:
                            # logger_lpd_noaa.info("SEASONALITY")
                            # logger_lpd_noaa.info(col)
                            e = str(col["seasonality"])
                        except KeyError:
                            e = ""
                    elif entry == "dataType":
                        e = self.noaa_data_sorted["Top"]["Data_Type"]
                    elif entry == "dataFormat":
                        # Lipd uses real data types (floats, ints), NOAA wants C or N (character or numeric)
                        if col[entry] == "float":
                            e = "N"
                        else:
                            e = "C"
                    else:
                        e = str(col[entry])
                    try:
                        e = e.replace(",", ";")
                    except AttributeError as ee:
                        print("write_variables_2: AttributeError: {}, {}".format(e, ee))
                    self.noaa_txt += '{}, '.format(e)
            except KeyError as e:
                self.noaa_txt += '{:<0}'.format(',')
                print("write_variables: KeyError: missing {}".format(e))
        self.noaa_txt += '\n#'
        return

    def __write_columns(self, pc, table):
        """
        Read numeric data from csv and write to the bottom section of the txt file.
        :param dict table: Paleodata dictionary
        :return none:
        """
        print("writing section: data, csv values from file")
        # get filename for this table's csv data
        # filename = self.__get_filename(table)
        # logger_lpd_noaa.info("processing csv file: {}".format(filename))
        # # get missing value for this table
        # # mv = self.__get_mv(table)
        # # write template lines
        # # self.__write_template_paleo(mv)
        if pc == "paleo":
            self.__write_template_paleo()
        elif pc == "chron":
            self.__write_template_chron()

        # continue if csv exists
        if self._values_exist(table):
            # logger_lpd_noaa.info("_write_columns: csv data exists: {}".format(filename))

            # sort the dictionary so the year column is first
            _csv_data_by_name = self.__put_year_col_first(table["columns"])

            # now split the sorted dictionary back into two lists (easier format to write to file)
            _names, _data = self.__rm_names_on_csv_cols(_csv_data_by_name)

            # write column variableNames
            self.__write_data_col_header(_names)

            # write data columns index by index
            self.__write_data_col_vals(_data)

        return

    def __write_template_top(self):
        """
        Write the template info for top section
        :return none:
        """
        self.noaa_txt += "\n#-----------------------------------------------------------------------\
            \n#                World Data Service for Paleoclimatology, Boulder\
            \n#                                  and\
            \n#                     NOAA Paleoclimatology Program\
            \n#             National Centers for Environmental Information (NCEI)\
            \n#-----------------------------------------------------------------------\
            \n# Template Version 3.0\
            \n# Encoding: UTF-8\
            \n# NOTE: Please cite Publication, and Online_Resource and date accessed when using these data. \
            \n# If there is no publication information, please cite Investigators, Title, and Online_Resource and date accessed."

        return

    def __write_template_variable(self):
        """
        Write the template info for the variable section
        :param str filename:
        :return none:
        """
        self.noaa_txt += '# Variables\
        \n#\n# Data variables follow that are preceded by "##" in columns one and two.\
        \n# Data line variables format:  Variables list, one per line, shortname-tab-longname-tab-longname components \
        ( 10 components: what, material, error, units, seasonality, data type, detail, method, C or N for Character or \
        Numeric data, additional_information)'
        self.noaa_txt += '\n#\n'

        return

    def __write_template_chron(self):
        """
        Write the chron header before the chron data columns.
        This is simpler than Paleo and needs to specify that it's a "chronology"
        :return:
        """
        self.noaa_txt += '# Chronology:\n'
        return

    def __write_template_paleo(self):
        """
        Write the template info for the paleo section
        :param str mv: Missing Value
        :return none:
        """
        self.noaa_txt += '# Data:\
        \n# Data lines follow (have no #)\
        \n# Data line format - tab-delimited text, variable short name as header)\
        \n# Missing_Values: nan\n#\n'
        return

    def __write_k_v(self, k, v, top=False, bot=False, multi=False, indent=False):
        """
        Write a key value pair to the output file. If v is a list, write multiple lines.
        :param k: Key
        :param v: Value
        :param bool top: Write preceding empty line
        :param bool bot: Write following empty line
        :param bool multi: v is a list
        :return none:
        """
        if top:
            self.noaa_txt += "\n#"
        if multi:
            for item in v:
                if indent:
                    self.noaa_txt += "\n#     {}: {}".format(str(k), str(item))
                else:
                    self.noaa_txt += "\n# {}: {}".format(str(k), str(item))
        else:
            if indent:
                self.noaa_txt += "\n#     {}: {}".format(str(k), str(v))
            else:
                self.noaa_txt += "\n# {}: {}".format(str(k), str(v))
        if bot:
            self.noaa_txt += "\n#"
        return

    def __write_header_name(self, name):
        """
        Write the title line for the section
        :return none:
        """
        self.noaa_txt += "# {}".format(name)
        return

    def __write_divider(self, top=False, bot=False):
        """
        Write a divider line
        :return none:
        """
        if top:
            self.noaa_txt += "\n#"
        self.noaa_txt += "\n#------------------\n"
        if bot:
            self.noaa_txt += "\n#"
        return

    def __write_data_col_header(self, l):
        """
        Write the variableNames that are the column header in the "Data" section
        :param list l: variableNames
        :return none:
        """
        count = len(l)
        for name in l:
            # last column - spacing not important
            if count == 1:
                self.noaa_txt += "{}\t".format(name)
            # all [:-1] columns - fixed spacing to preserve alignment
            else:
                # FIX THIS TO TABS ONLY
                self.noaa_txt += "{}\t".format(name)
                count -= 1
        self.noaa_txt += '\n'

    def __write_data_col_vals(self, ll):
        """
        Loop over value arrays and write index by index, to correspond to the rows of a txt file
        :param list ll: List of lists, column data
        :return:
        """

        # all columns should have the same amount of values. grab that number
        try:
            _items_in_cols = len(ll[0]["values"])
            for idx in range(0, _items_in_cols):
                # amount of columns
                _count = len(ll)
                for col in ll:
                    # last item in array: fixed spacing not needed
                    if _count == 1:
                        self.noaa_txt += "{}\t".format(str(col["values"][idx]))
                    # all [:-1] items. maintain fixed spacing
                    else:
                        self.noaa_txt += "{}\t".format(str(col["values"][idx]))
                    _count -= 1
                self.noaa_txt += '\n'

        except IndexError:
            print("_write_data_col_vals: IndexError: couldn't get length of columns")

        return