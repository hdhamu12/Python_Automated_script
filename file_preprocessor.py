# this file is used to call the respective class of the preprocessor defined in the preprocessor_config column
# based on the class value in the preprocessor_config column the class can be called.
import abc
from abc import ABC, abstractmethod
import json
from loggers import gdp_logger
import os
import zipfile
from datetime import datetime
import codecs
import re
import pandas as pd
import xlrd as xl
import csv

Logger = gdp_logger.create_Logger(__name__)
tdate = datetime.today().strftime("%Y-%m-%d")


class Preprocessor(metaclass=abc.ABCMeta):
    def __init__(self, p_params, file_path=None, filename=None):
        super().__init__()
        # this p_params will be accessible to all the classes derived
        self.v_params = p_params

    # defining predefined functions to be used in various subclasses
    # we can use this functions directly in the subclasses (see line no. 94)

    def unzip_files_pre_processor(self):
        with zipfile.ZipFile(
            self.v_params["filepath"] + self.v_params["filename"], "r"
        ) as zip_ref:
            zip_ref.extractall(self.v_params["filepath"])

    """
     Defining the function to convert excel to csv
     this function requires various parameters like file_path, filename, sheet_name,
     sheet_index, keep_header, skip_rows, file_sep=','
     this above parameters need to be passed from outside the function either using dictionary
    """

    def convert_excel_to_csv(self):
        file_path = self.v_params["filepath"]
        filename = self.v_params["filename"]

        csv_header = []
        try:
            wb = xl.open_workbook(file_path + filename)
            s1 = wb.sheet_by_index(self.v_params["sheet_name"])
            s1.cell_value(0, 0)
            if self.v_params["skiprows"] == 0:
                read_file = pd.read_excel(
                    file_path + filename, sheet_name=self.v_params["sheet_name"]
                )
            else:
                read_file = pd.read_excel(
                    file_path + filename,
                    sheet_name=self.v_params["sheet_name"],
                    skiprows=range(1, self.v_params["skiprows"]),
                )

            read_file.to_csv(
                file_path
                + filename.replace(".xls", "_temp.csv").replace(".xlsx", "_temp.csv"),
                header=bool(self.v_params["keepheader"]),
                mode="a",
                index=None,
            )

            if not self.v_params["keepheader"]:
                for i in range(1, s1.ncols + 1):
                    csv_header.append("C" + str(i))
                    listToStr = ",".join([str(elem) for elem in csv_header])
                with open(
                    file_path
                    + filename.replace(".xls", ".csv").replace(".xlsx", ".csv"),
                    "w",
                    encoding="utf8",
                ) as f:
                    f.write(listToStr)
                    f.write("\n")
                f.close()
                df = pd.read_csv(
                    file_path
                    + filename.replace(".xls", "_temp.csv").replace(
                        ".xlsx", "_temp.csv"
                    )
                )
                df.to_csv(
                    file_path
                    + filename.replace(".xls", ".csv").replace(".xlsx", ".csv"),
                    index=False,
                    sep=(self.v_params["filesep"]),
                    mode="a",
                )
                os.remove(
                    file_path
                    + filename.replace(".xls", "_temp.csv").replace(
                        ".xlsx", "_temp.csv"
                    )
                )

            else:

                df = pd.read_csv(
                    file_path
                    + filename.replace(".xls", ".csv").replace(".xlsx", ".csv")
                )
                df.to_csv(
                    file_path
                    + filename.replace(".xls", ".csv").replace(".xlsx", ".csv"),
                    index=False,
                    sep=(self.v_params["filesep"]),
                )

            return filename.replace(".xls", ".csv").replace(".xlsx", ".csv")
        except Exception as e:
            Logger.error(e)
            print("exiting preprocessor with error")

            return

    @abstractmethod
    def preprocess(self):
        """abstract method"""


# defining the various preprocessor classes required.


class AuroaeraPreprocessor(Preprocessor):
    # redefining the preprocess function for the overriding

    def preprocess(self):
        with open(
            self.v_params["filepath"] + "/" + self.v_params["filename"], "r"
        ) as fp:

            f = open(self.v_params["filepath"] + "/" + self.v_params["filename"], "rt")
            data = f.read()
            data = data.replace('"""', '"')
            data = data.replace('"," ', " -")
            f.close()

            f = open(
                self.v_params["filepath"]
                + self.v_params["filename"].replace(".csv", "_PP.csv"),
                "wt",
            )
            f.write(data)
            f.close()

        return self.v_params["filename"].replace(".csv", "_PP.csv")


class EMCSGPreprocessor(Preprocessor):

    # redefining the preprocess function for the overriding

    def preprocess(self):

        with zipfile.ZipFile(
            self.v_params["filepath"] + self.v_params["filename"], "r"
        ) as zip_ref:
            zip_ref.extractall(self.v_params["filepath"])
        final_filename = self.v_params["filename"].replace(".zip", ".csv")
        source_name = self.v_params["filename"][
            0 : self.v_params["filename"].find("_", 1)
        ]
        date_part = (
            self.v_params["filename"]
            .replace(source_name, "")
            .replace("_for_", "")
            .replace(".zip", "")
        )
        new_filename = source_name + "_" + date_part + "_to_" + date_part + ".csv"
        print(
            "Renaming actual file "
            + new_filename
            + " to required format: "
            + final_filename
        )
        if os.path.exists(self.v_params["filepath"] + new_filename):
            os.rename(
                self.v_params["filepath"] + new_filename,
                self.v_params["filepath"] + final_filename,
            )
        else:
            print("Required file does not exists:-> " + new_filename)
        os.remove(self.v_params["filepath"] + self.v_params["filename"])
        return final_filename


class IsonePreprocessor(Preprocessor):
    def preprocess(self):
        json_file = open(self.v_params["filepath"] + self.v_params["filename"], "r")
        json_data = json.load(json_file)
        json_file.close()

        f = open(
            self.v_params["filepath"]
            + (self.v_params["filename"]).replace(".json", "_PP.csv"),
            "w+",
        )
        for rows in json_data["HourlyLmps"]["HourlyLmp"]:
            f.write(
                rows["BeginDate"]
                + ";"
                + str(rows["LmpTotal"])
                + ";"
                + str(rows["Location"]["@LocId"])
                + ";"
                + str(rows["EnergyComponent"])
                + ";"
                + str(rows["CongestionComponent"])
                + ";"
                + str(rows["LossComponent"])
            )
            f.write("\n")
        f.close()

        return (self.v_params["filename"]).replace(".json", "_PP.csv")


class ECBPreprocessor(Preprocessor):
    def preprocess(self):
        file_extension = ".zip"
        Preprocessor.unzip_files_pre_processor(self)

        date_string = datetime.strptime(
            str(datetime.now()), "%Y-%m-%d %H:%M:%S.%f"
        ).strftime("%Y%m%d")

        os.rename(
            self.v_params["filepath"]
            + (self.v_params["filename"]).replace(
                "_" + str(date_string) + file_extension, ".csv"
            ),
            self.v_params["filepath"]
            + (self.v_params["filename"]).replace(file_extension, "_PP" + ".csv"),
        )
        os.remove(self.v_params["filepath"] + (self.v_params["filename"]))

        return (self.v_params["filename"]).replace(file_extension, "_PP" + ".csv")


class epsis_preprocessor_v1(Preprocessor):
    def preprocess(self):
        file_extension = ".csv"
        datasourcedict = {}
        keys = []
        parsedRow_towrite = ""

        with open(
            self.v_params["filepath"]
            + str(self.v_params["filename"]).replace(
                file_extension, "_PP" + file_extension
            ),
            "a",
            newline="",
            encoding="utf-8",
        ) as fw:

            # used codecs to open the file due to Korean characters
            br = codecs.open(
                self.v_params["filepath"] + self.v_params["filename"], "r", "utf-8"
            )
            for line in br.readlines():

                if line != "":
                    rows = line.split(";")
                    for row in rows:
                        if row.find("textFormmat") != -1:
                            cells = row.split("=")
                            key = cells[0].strip()
                            keys.append(key)
                            value = cells[1][
                                cells[1].index('"') + 1 : cells[1].rindex('"')
                            ]
                            datasourcedict[key] = value
                        elif row.find("localnm=") != -1:
                            cells = row.split("=")
                            key = cells[0].strip()
                            keys.append(key)
                            datasourcedict[key] = cells[1].replace('"', "")
                        elif row.find("year") != -1 and row.find("/") != -1:
                            str_row = row.split("/")
                            year = re.sub("[^0-9]+", "", str_row[0]).strip()
                            month = re.sub("[^0-9]+", "", str_row[1]).strip()
                            parsedRow_towrite = year + "," + month
                        if row.find("gridData.push") != -1:

                            # using an additional list 'Keys' to iterate through dictionary because
                            # need to iterate through dictionary keys in the same order as they were appended
                            # to maintain column order in the resultant file
                            for key in keys:
                                # for data_key, data_values in sorted(datasourcedict.items()):
                                parsedRow_towrite = (
                                    parsedRow_towrite + "," + datasourcedict[key]
                                )
                            fw.write(parsedRow_towrite)
                            fw.write("\n")
                            parsedRow_towrite = ""
                            year = ""
                            month = ""
                            datasourcedict.clear()
                            keys = []

            br.close()
        fw.close()
        return str(self.v_params["filename"]).replace(
            file_extension, "_PP" + file_extension
        )


class epsis_preprocessor_v2(Preprocessor):
    def preprocess(self):
        file_extension = ".csv"
        datasourcedict = {}
        keys = []
        parsedRow_towrite = ""

        with open(
            self.v_params["filepath"]
            + self.v_params["filename"].replace(file_extension, "_PP" + file_extension),
            "w",
            newline="",
            encoding="utf-8",
        ) as fw:
            # with open(file_path+file, encoding='utf-8') as br:
            # used codecs to open the file due to Korean characters
            br = codecs.open(
                self.v_params["filepath"] + self.v_params["filename"], "r", "utf-8"
            )
            for line in br.readlines():

                if line != "":
                    rows = line.split(";")
                    for row in rows:
                        if row.find("textFormmat") != -1:
                            cells = row.split("=")
                            key = cells[0].strip()
                            keys.append(key)
                            value = cells[1][
                                cells[1].index('"') + 1 : cells[1].rindex('"')
                            ]
                            datasourcedict[key] = value
                        elif row.find("localnm=") != -1:
                            cells = row.split("=")
                            key = cells[0].strip()
                            keys.append(key)
                            datasourcedict[key] = cells[1].replace('"', "")
                        elif row.find("year") != -1 and row.find("/") != -1:
                            str_row = row.split("/")
                            year = re.sub("[^0-9]+", "", str_row[0]).strip()
                            month = re.sub("[^0-9]+", "", str_row[1]).strip()
                            parsedRow_towrite = year + "," + month
                        if row.find("gridData.push") != -1:

                            # using an additional list 'Keys' to iterate through dictionary because
                            # need to iterate through dictionary keys in the same order as they were appended
                            # to maintain column order in the resultant file
                            for key in keys:
                                # for data_key, data_values in sorted(datasourcedict.items()):
                                parsedRow_towrite = (
                                    parsedRow_towrite + "," + datasourcedict[key]
                                )
                            fw.write(parsedRow_towrite)
                            fw.write("\n")
                            parsedRow_towrite = ""
                            year = ""
                            month = ""
                            datasourcedict.clear()
                            keys = []

            br.close()
        fw.close()
        return str(self.v_params["filename"]).replace(
            file_extension, "_PP" + file_extension
        )


# to be discussed with alex on how to do the implementation.


class kpx_preprocessor_v1(Preprocessor):
    def preprocess(self):
        filename = Preprocessor.convert_excel_to_csv(self)

        if filename is not None:
            try:
                os.rename(
                    self.v_params["filepath"] + self.v_params["filename"],
                    self.v_params["filepath"]
                    + (self.v_params["filename"]).replace(".csv", "_PP" + ".csv"),
                )

                os.remove(self.v_params["filepath"] + (self.v_params["filename"]))

                file = open(
                    self.v_params["filepath"] + filename.replace(".csv", "_PP" + ".csv")
                )
                reader = csv.reader(file)
                lines = len(list(reader))
                if lines <= 1:
                    raise Exception("Empty File.")
            except Exception as e:
                Logger.info("error while preprocessing" + e)
                return filename.replace(".csv", "_PP" + ".csv")
            except FileNotFoundError as e:
                Logger.info(f"FileNotFoundError successfully handled\n" f"{e}")

            return filename.replace(".csv", "_PP" + ".csv")
        else:
            Logger.error("file after preprocessing not found")


# ####todo the above changes need to be replicated here in this function as well
class kpx_preprocessor_v2(Preprocessor):
    def preprocess(self):
        filename = Preprocessor.convert_excel_to_csv(self)

        if filename is not None:
            try:
                os.rename(
                    self.v_params["filepath"] + filename,
                    self.v_params["filepath"]
                    + filename.replace(".csv", "_PP" + ".csv"),
                )
                os.remove(self.v_params["filepath"] + self.v_params["filename"])

                file = open(
                    self.v_params["filepath"] + filename.replace(".csv", "_PP" + ".csv")
                )
                reader = csv.reader(file)
                lines = len(list(reader))
                if lines <= 1:
                    raise Exception("Empty File.")
                return filename.replace(".csv", "_PP" + ".csv")
            except Exception as e:
                Logger.info("error while preprocessing" + e)
                return filename.replace(".csv", "_PP" + ".csv")
            except FileNotFoundError as e:
                Logger.info(f"FileNotFoundError successfully handled\n" f"{e}")
            return filename.replace(".csv", "_PP" + ".csv")
        else:
            Logger.error("file after preprocessing not found")


""" This is the preprocessor for entsoe datasets"""


class entsoe_preprocessor(Preprocessor):
    def preprocess(self):
        file_extension = ".csv"
        try:
            with open(
                self.v_params["filepath"] + self.v_params["filename"],
                "r",
                encoding="utf8",
            ) as f:
                text = f.read()
            # process Unicode text
            with open(
                self.v_params["filepath"]
                + (self.v_params["filename"]).replace(
                    file_extension, "_PP" + file_extension
                ),
                "w",
                encoding="utf8",
            ) as w:
                w.write(text.replace("\r\n", "\n"))
            os.remove(self.v_params["filepath"] + self.v_params["filename"])
            return (self.v_params["filename"]).replace(
                file_extension, "_PP" + file_extension
            )
        except Exception as e:
            Logger.error(e)
        finally:
            f.close()
            w.close()


class ihs_preprocessor_v1(Preprocessor):
    def preprocess(self):

        file_path = self.v_params["filepath"]
        file = self.v_params["filename"]

        csv_filename = Preprocessor.json_to_csv_convertor(file_path, file)
        with open(
            file_path + csv_filename.replace(".csv", "_PP.csv"), "w", encoding="utf8"
        ) as f:
            with open(file_path + csv_filename, "r", encoding="utf8") as a_file:
                for row in a_file:
                    if row.count(",") < 8:
                        # r = row.split(",", 4)[4]
                        # row = row[0:len(row)-len(r)] + " ," + r
                        continue
                    f.write(row)
        f.close()
        return csv_filename.replace(".csv", "_PP.csv")


class ihs_preprocessor_v2(Preprocessor):
    def preprocess(self):
        file = self.v_params["filename"]
        file_path = self.v_params["filepath"]
        if "plants_metadata" in file:
            print("running the preprocessor for ihs metadata")
            f = open(file_path + file)
            j = json.load(f)
            f.close()
            f1 = open(file_path + "plants_metadata_" + tdate + ".csv", "w+")
            for each in j[2]["mappings"]:
                row = (
                    str(each["id"])
                    + ","
                    + str(each["name"])
                    + ","
                    + str(each["region"])
                    + ","
                    + str(each["groupings"]["technology"])
                    .replace("[", "")
                    .replace("]", "")
                    .replace("'", "")
                )
                f1.write(row)
                f1.write("\n")
            f1.close()
            filename = "plants_metadata_" + tdate + ".csv"
        elif (
            "forecast_monthly_prices" in file
            or "forecast_yearly_prices" in file
            or "offtake_contracts" in file
            or "ihs_historical_prices" in file
        ):
            data = pd.read_json(file_path + file)
            filename = file.replace(".json", tdate + "_PP.csv")
            data.to_csv(
                file_path + filename,
                quotechar='"',
                index=False,
                quoting=csv.QUOTE_NONNUMERIC,
            )
        return filename
