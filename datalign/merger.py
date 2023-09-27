import warnings
import numpy as np
import os

import pandas as pd
import yaml
import xlsxwriter
import numpy as np
from dateutil import parser
import os
import utils

pd.set_option("display.max_columns", None)
# pd.set_option('display.max_rows', None)
pd.set_option("display.max_colwidth", None)
pd.options.mode.chained_assignment = None


class Config:
    MAPKEY = "id_key"

    def __init__(self, cfg, cfgpath=None):
        self._cfg = cfg
        self._cfgpath = cfgpath

    @classmethod
    def from_filename(cls, filename):
        with open(filename, "r") as ymlfile:
            cfg = yaml.full_load(ymlfile)
        return cls(cfg, cfgpath=filename)

    def set_value(self, key=None, **kwargs):
        data = self.cfg
        if key == None:
            for k, v in kwargs.items():
                data[k] = v
            with open(self._cfgpath, "w") as yaml_file:
                yaml_file.write(yaml.dump(data, default_flow_style=False))
            self._cfg = data
        else:
            for k, v in kwargs.items():
                data[key][k] = v
            with open(self._cfgpath, "w") as yaml_file:
                yaml_file.write(yaml.dump(data, default_flow_style=False))
            self._cfg = data

    def create_recap(self, overwrite=False):
        if not hasattr(self, "_recap") or overwrite == True:
            recap = {}
            s1_prefix, s2_prefix = (
                self.cfg["source1"]["prefix"],
                self.cfg["source2"]["prefix"],
            )
            recap[s1_prefix] = {}
            recap[s2_prefix] = {}
            self._recap = recap
        else:
            warnings.warn("recap already exists.")

    def update_recap(self, key, *args):
        for k, v in zip(self.recap.keys(), args):
            self._recap[k][key] = v

    def get_prefix(self, source="source1"):
        return self.cfg[source]["prefix"]

    @property
    def cfg(self):
        return self._cfg

    @property
    def source1(self):
        return self._cfg["source1"]

    @property
    def source2(self):
        return self._cfg["source2"]

    @property
    def options(self):
        return self._cfg["options"]

    @property
    def idmapping(self):
        return self._cfg["id_mapping"]

    @property
    def cfgpath(self):
        return self._cfgpath

    @property
    def debug(self):
        return self.cfg["options"]["debug"]

    @property
    def aggregate(self):
        return self.cfg["options"]["aggregate_exports"]

    @property
    def aggregate_values(self):
        return self.cfg["options"]["aggregate_values"]

    @property
    def recap(self):
        if not hasattr(self, "_recap"):
            self.create_recap()
        return self._recap


class Mapper:
    def __init__(self, mapp, source1, source2, mappath=None):
        self._map = mapp
        self._initmap = mapp
        self._mappath = mappath
        self._source1 = source1
        self._source2 = source2

    @classmethod
    def from_path(cls, mappath, source1, source2, sep=";", dtypes=None):
        df = pd.read_csv(mappath, sep=sep, dtype=dtypes, keep_default_na=False)
        return cls(df, mappath=mappath, source1=source1, source2=source2)

    @staticmethod
    def has_field(df, field):
        if field in df.columns:
            return True
        return False

    def preprocess(self, apply_mapping=True, lowerall=True):
        if lowerall:
            self._map.columns = self._map.columns.str.lower()
            self._map = self._map.apply(lambda x: x.astype(str).str.lower())

        self._map.columns = self._map.columns.str.lower()
        self._map = self._map.fillna(value="")
        for cl in list(self._map.select_dtypes(include=["object"]).columns):
            self._map[cl] = self._map[cl].map(lambda x: x.lower())

        self._map[self.source1.prefix + "?"] = self._map[self.source1.map_col].apply(
            lambda x: self.has_field(self.source1.df, x)
        )
        map_fields = [self.source1.map_col, self.source2.map_col, "type"]

        self._map[self.source2.prefix + "?"] = self._map[self.source2.map_col].apply(
            lambda x: self.has_field(self.source2.df, x)
        )

        self._full_map_report = self._map
        self._map = self._map[
            (self._map[self.source1.prefix + "?"] == True)
            & (self._map[self.source2.prefix + "?"] == True)
        ]

        self._fix_multiple_mapping()

        self._source1_fields = self._map[self.source1.map_col]
        self._source2_fields = self._map[self.source2.map_col]
        self._source_com = self._source1_fields.apply(lambda x: x + "?")

        if apply_mapping:
            self._apply_map_onsources()

    def _fix_multiple_mapping(self):
        dup_count_s1 = self._map.groupby(
            self._map[self.source1.map_col].tolist(), as_index=False
        ).size()
        self._duplicate_source1 = dup_count_s1[dup_count_s1["size"] > 1]

        for idx, row in self._duplicate_source1.iterrows():
            for i in range(row["size"]):
                try:
                    if i > 0:
                        self.source1._df[
                            row["index"] + "_" + str(i)
                        ] = self.source1._df[row["index"]]
                except KeyError:
                    if self.source1.cfg.debug:
                        print("\tError at row :")
                        print(row["index"])

        self._s = self._map.groupby(self.source1.map_col).cumcount()
        self._map[self.source1.map_col] = self._map[self.source1.map_col].where(
            self._s.eq(0), self._map[self.source1.map_col] + "_" + self._s.astype(str)
        )

    def _apply_map_onsources(self):
        source1 = self.source1.df[self.source1_fields].applymap(
            lambda s: s.lower() if type(s) == str else s
        )
        source2 = self.source2.df[self.source2_fields].applymap(
            lambda s: s.lower() if type(s) == str else s
        )
        mapping_fields = [None] * (len(self.source1_fields) + len(self.source2_fields))
        print(
            f" Source2 fields : {self.source2_fields} len = {len(self.source2_fields)}"
        )
        print(
            f" Source2 labels fields : {list(self.prefixcol_s2.values())} len = {len(list(self.prefixcol_s2.values()))}"
        )

        print(
            f" Source1 fields : {self.source1_fields} len = {len(self.source1_fields)}"
        )
        print(
            f" Source1 labels fields : {list(self.prefixcol_s1.values())} len = {len(list(self.prefixcol_s1.values()))}"
        )
        mapping_fields[::2] = list(self.prefixcol_s1.values())
        mapping_fields[1::2] = list(self.prefixcol_s2.values())
        self._mappedsource1 = source1.rename(columns=self.prefixcol_s1)
        self._mappedsource2 = source2.rename(columns=self.prefixcol_s2)
        self._mapping_fields = mapping_fields

        self.source1._mappeddf = self.mapped_source1
        self.source2._mappeddf = self.mapped_source2

    def parse_dates(self):
        if "type" not in self._map.columns:
            self._map["type"] = [np.nan] * len(self._map)
        s1_dates = (
            self._map[self._map["type"] == "date"][self.source1.map_col]
            .apply(lambda x: self.source1.prefix + x)
            .tolist()
        )
        s2_dates = (
            self._map[self._map["type"] == "date"][self.source2.map_col]
            .apply(lambda x: self.source2.prefix + x)
            .tolist()
        )
        if len(s1_dates) > 0:
            for date_field in s1_dates:
                try:
                    self._mappedsource1[date_field] = self.mapped_source1[
                        date_field
                    ].apply(lambda x: parser.parse(x).date() if x != "" else x)
                except:
                    warnings.warn("Could not parse date")

            for date_field in s2_dates:
                try:
                    self._mappedsource2[date_field] = self._mappedsource2[
                        date_field
                    ].apply(lambda x: parser.parse(x).date() if x != "" else x)
                except:
                    warnings.warn("Could not parse date")

        self._dates = {"source1": s1_dates, "source2": s2_dates}

    @property
    def initmap(self):
        return self._initmap

    @property
    def mapdf(self):
        return self._map

    @property
    def mappath(self):
        return self._mappath

    @property
    def source1(self):
        return self._source1

    @property
    def source2(self):
        return self._source2

    @property
    def mapped_source1(self):
        return self._mappedsource1

    @property
    def mapped_source2(self):
        return self._mappedsource2

    @property
    def full_map_report(self):
        return self._full_map_report

    @property
    def duplicate_source1(self):
        return self._duplicate_source1

    @property
    def source1_fields(self):
        return self._source1_fields

    @property
    def source2_fields(self):
        return self._source2_fields

    @property
    def source_com(self):
        return self._source_com

    @property
    def prefixcol_s1(self):
        return {x: self.source1.prefix + x for x in self.source1_fields}

    @property
    def prefixcol_s2(self):
        return {x: self.source2.prefix + x for x in self.source2_fields}

    @property
    def _s1count(self):
        return self._s

    @property
    def mapping_fields(self):
        return self._mapping_fields

    @property
    def dates(self):
        if hasattr(self, "_dates"):
            return self._dates
        elif "type" not in self._map.columns:
            warnings.warn('Missing "type" column in mapping dataframe.')
        else:
            s1_dates = (
                self._map[self._map["type"] == "date"][self.source1.map_col]
                .apply(lambda x: self.source1.prefix + x)
                .tolist()
            )
            s2_dates = (
                self._map[self._map["type"] == "date"][self.source2.map_col]
                .apply(lambda x: self.source2.prefix + x)
                .tolist()
            )
            self._dates = {"source1": s1_dates, "source2": s2_dates}
            return self._dates

    @property
    def full_id_map(self):
        return self._full_id_map


class SourceDF:
    def __init__(self, df, cfg, dfpath=None, source=None):
        self._df = df
        self._initdf = df
        self._dfpath = dfpath
        self._cfg = cfg
        self._source = source

    @classmethod
    def from_path(cls, dfpath, cfg, sep=";", kind="csv", source=None, dtypes=None):
        cls._dfpath = dfpath
        if kind == "csv":
            df_tmp = pd.read_csv(dfpath, sep=sep, keep_default_na=False, nrows=2)
            df = pd.read_csv(
                dfpath,
                sep=sep,
                dtype={k: str for k in df_tmp.columns},
                keep_default_na=False,
            )
        elif kind == "excel":
            df = pd.read_excel(dfpath, keep_default_na=False)
        return cls(df, cfg=cfg, dfpath=dfpath, source=source)

    def preprocess(self, fullidmap_filename=None, sep=None, lowerall=True):
        source = self.sourcekey

        if lowerall:
            self._df.columns = self._df.columns.str.lower()
            self._df = self._df.apply(lambda x: x.astype(str).str.lower())

        df = self._df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self._initdf = df.copy()

        self._df = df

        if self.cfg.options["use_full_id_map"]:
            self._use_full_id_map(filename=fullidmap_filename, sep=sep)
            self._pivot = self.cfg.MAPKEY
        else:
            self._pivot = self.cfg.cfg[self.sourcekey]["pivot_field"]

        self._nullpivot = self._df[
            self._df[self._pivot].isin([np.nan, "", "NaN", "nan", "Nan", pd.NaT])
        ]

        self._df = self._df[
            ~self._df[self._pivot].isin([np.nan, "", "NaN", "nan", "Nan", pd.NaT])
        ]

        self._duplicatepivots = self._df[
            self._df.duplicated(subset=[self._pivot], keep=False)
        ]
        self._lenuniqueduplicate = len(
            self._df[self._df.duplicated(subset=[self._pivot], keep="first")]
        )
        self._df.drop_duplicates(self._pivot, keep="first", inplace=True)

    def _use_full_id_map(self, filename, sep=";"):
        source1_id_in_map = self.cfg.idmapping["source1_id"]
        source2_id_in_map = self.cfg.idmapping["source2_id"]

        map_key = self.cfg.MAPKEY
        self._full_id_map = pd.read_csv(filename, sep=sep)[
            [source1_id_in_map, source2_id_in_map, map_key, "source"]
        ]
        self._full_id_map = self._full_id_map.astype(str)
        self._df = self._df.astype(str)
        if self.sourcekey == "source1":
            left_on = self.cfg.source1["id_field"]
            self._df = self._df.merge(
                self._full_id_map,
                left_on=left_on,
                right_on=source1_id_in_map,
                how="left",
            )
        else:
            left_on = self.cfg.source2["id_field"]
            self._df = self._df.merge(
                self._full_id_map,
                left_on=left_on,
                right_on=source2_id_in_map,
                how="left",
            )

    def has_field(self, field):
        if field in self.df.columns:
            return True
        return False

    @staticmethod
    def get_col_widths(df):
        # First we find the maximum length of the index column
        idx_max = max(
            [len(str(s)) for s in df.index.values] + [len(str(df.index.name))]
        )
        # Then, we concatenate this to the max of the lengths of column name and its values for each column, left to right
        return [idx_max] + [
            max([len(str(s)) for s in df[col].values] + [len(col)])
            for col in df.columns
        ]

    @property
    def df(self):
        return self._df

    @property
    def initdf(self):
        return self._initdf

    @property
    def cfg(self):
        return self._cfg

    @property
    def prefix(self):
        return self.cfg.get_prefix(self.sourcekey)

    @property
    def map_col(self):
        return self.cfg.cfg[self.sourcekey]["mapping_col_name"]

    @property
    def source(self):
        return self._source

    @property
    def pivot(self):
        if hasattr(self, "_pivot"):
            return self._pivot
        else:
            return None

    @property
    def sourcekey(self):
        if self.source == "src":
            source = "source1"
        elif self.source == "dest":
            source = "source2"
        else:
            source = self.source
        return source


class Merger:
    def __init__(self, mapper, exportpathdir="export/"):
        self._mapper = mapper
        self._source1 = self.mapper.source1
        self._source2 = self.mapper.source2
        if not os.path.isdir(exportpathdir):
            os.mkdir(exportpathdir)
        self._exportpathdir = exportpathdir

    def merge_sources(self):
        match = self.mapper.mapped_source1.merge(
            self.mapper.mapped_source2,
            left_on=self.mapper.prefixcol_s1[self.source1.pivot],
            right_on=self.mapper.prefixcol_s2[self.source2.pivot],
            how="inner",
            suffixes=("", "_2"),
        )[self.mapper.mapping_fields]

        if len(match) == 0:
            print("No match found")
        self._match = match

    @staticmethod
    def truncate(f, n):
        from math import floor

        return floor(f * 10**n) / 10**n

    def process_basic_analysis(self):
        self._comp_table = pd.DataFrame()
        for x, y, z in zip(
            list(self.mapper.prefixcol_s1.values()),
            list(self.mapper.prefixcol_s2.values()),
            self.mapper.source_com.tolist(),
        ):
            try:
                self._comp_table[z] = self.match[x] == self.match[y]
            except (TypeError, ValueError):
                if self.source1.cfg.debug:
                    print("\tCheck fields :")
                    print(x, y)

        self._comp_table.reset_index(drop=True, inplace=True)

        desc = self._comp_table.describe()

        # X is "top", so either true or false, i.e. 1 or 0
        # f X |-> 1-X - (-2X+1)(freq)
        # gives freq   if X=1
        # gives 1-freq if X=0
        desc.loc["match"] = (1 - desc.loc["top"]) * desc.loc["count"] - (
            -2 * desc.loc["top"] + 1
        ) * (desc.loc["freq"])
        desc.loc["ratio"] = desc.loc["match"] / desc.loc["count"]
        try:
            desc.loc["ratio"] = desc.loc["ratio"].apply(lambda x: self.truncate(x, 3))
        except ValueError:
            warnings.warn("There is no match!")
        self._desc = desc
        self._match.drop_duplicates(
            keep="first",
            subset=[self.source1.prefix + self.source1.pivot],
            inplace=True,
        )

    def build_excel_filename(
        self, aggregate=False, aggregate_value=None, excel_name=None
    ):
        if excel_name is None:
            excel_name = self.source1.cfg.options["field_comparison_export_filename"]
        if aggregate:
            self._excel_filename = f"{aggregate_value}_{excel_name}.xlsx"
        else:
            self._excel_filename = f"{excel_name}.xlsx"

    def export_pivot_field_matching(self):
        s1_id_field = self.source1.cfg.source1["id_field"]
        s2_id_field = self.source2.cfg.source2["id_field"]

        id_mapping = self.match
        id_mapping["matching_source"] = f"{self.source2.pivot}_match"

        if os.path.isfile(
            os.path.join(self.exportpathdir, f"{self.source2.pivot}_match.csv")
        ):
            self._old_map = pd.read_csv(f"{self.source2.pivot}_match.csv", sep=";")
            id_mapping = pd.concat([self._old_map, id_mapping]).reset_index(drop=True)

        id_mapping = id_mapping.astype(str)
        self._id_mapping = id_mapping.drop_duplicates(
            subset=[
                self.source1.prefix + s1_id_field,
                self.source2.prefix + s2_id_field,
            ],
            keep="first",
        )

        self._subset = [
            self.source1.prefix + s1_id_field,
            self.source2.prefix + s2_id_field,
        ]

        self.id_mapping.to_csv(
            os.path.join(self.exportpathdir, f"{self.source2.pivot}_match.csv"),
            sep=";",
            index=False,
        )

    def write_excel(
        self,
        metrics_sheet=True,
        match_sheet=True,
        writer=None,
        mode="w",
        pivot_alias=None,
    ):
        if writer == None:
            writer = pd.ExcelWriter(
                os.path.join(self.exportpathdir, f"{self.excel_filename}"),
                engine="xlsxwriter",
                if_sheet_exists=None,
                mode=mode,
            )

        workbook = writer.book

        if match_sheet:
            if pivot_alias != None:
                sheet_name = utils.remove_invalid_characters(f"{pivot_alias}_match")
            else:
                sheet_name = utils.remove_invalid_characters(
                    f"{self.source2.pivot}_match"
                )
            self.match.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = self.match.shape
            if mode == "w" and max_row > 0:
                for i in range(max_col):
                    col_name = xlsxwriter.utility.xl_col_to_name(i)
                    if i % 2 == 0:
                        col_near = xlsxwriter.utility.xl_col_to_name(i + 1)
                    else:
                        col_near = xlsxwriter.utility.xl_col_to_name(i - 1)

                    crit_eq = f"={col_name}2:{col_name}{max_row}={col_near}2:{col_near}{max_row}"
                    crit_dif = f"={col_name}2:{col_name}{max_row}<>{col_near}2:{col_near}{max_row}"

                    worksheet.conditional_format(
                        1,
                        i,
                        max_row,
                        i,
                        {
                            "type": "formula",
                            "criteria": crit_eq,
                            "format": utils.get_workbookformat(workbook, kind="good"),
                        },
                    )
                    worksheet.conditional_format(
                        1,
                        i,
                        max_row,
                        i,
                        {
                            "type": "formula",
                            "criteria": crit_dif,
                            "format": utils.get_workbookformat(workbook, kind="miss"),
                        },
                    )
        if metrics_sheet:
            if pivot_alias != None:
                sheet_name = utils.remove_invalid_characters(f"{pivot_alias}_metrics")
            else:
                sheet_name = utils.remove_invalid_characters(
                    f"{self.source2.pivot}_metrics"
                )
            self.metrics.to_excel(writer, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            if mode == "w" and len(self.match) > 0:
                for i, width in enumerate(self.source1.get_col_widths(self.metrics)):
                    worksheet.set_column(i, i, width)
                    max_met_col = self.metrics.columns.size

                    worksheet.conditional_format(
                        3,
                        1,
                        3,
                        max_met_col,
                        {
                            "type": "cell",
                            "criteria": ">=",
                            "value": 1,
                            "format": utils.get_workbookformat(
                                workbook, kind="perfect"
                            ),
                        },
                    )
                    worksheet.conditional_format(
                        3,
                        1,
                        3,
                        max_met_col,
                        {
                            "type": "cell",
                            "criteria": ">=",
                            "value": 0.8,
                            "format": utils.get_workbookformat(workbook, kind="good"),
                        },
                    )
                    worksheet.conditional_format(
                        3,
                        1,
                        3,
                        max_met_col,
                        {
                            "type": "cell",
                            "criteria": ">=",
                            "value": 0.3,
                            "format": utils.get_workbookformat(workbook, kind="wrong"),
                        },
                    )
                    worksheet.conditional_format(
                        3,
                        1,
                        3,
                        max_met_col,
                        {
                            "type": "cell",
                            "criteria": "<",
                            "value": 0.3,
                            "format": utils.get_workbookformat(workbook, kind="miss"),
                        },
                    )
        self._workbook = workbook
        self._writer = writer

    def export_mismatch(
        self,
        recap=None,
        export_missingmap=True,
        sheet_name="fields_mapping",
        close_writer=False,
    ):
        if not self.source1.cfg.options["use_full_id_map"]:
            self._mismatch_src1 = self.mapper.mapped_source1[
                ~self.mapper.mapped_source1[
                    self.mapper.prefixcol_s1[self.source1.pivot]
                ].isin(self.match[self.source1.prefix + self.source1.pivot])
            ]
            self._mismatch_src2 = self.mapper.mapped_source2[
                ~self.mapper.mapped_source2[
                    self.mapper.prefixcol_s2[self.source2.pivot]
                ].isin(self.match[self.source2.prefix + self.source2.pivot])
            ]

        else:
            self._build_mismatch_usefullid(export_missingmap=export_missingmap)

        sheet_name2 = self.source2.prefix + "WARNING_mismatch"
        sheet_name1 = self.source1.prefix + "WARNING_mismatch"

        sheet_name1 = utils.remove_invalid_characters(sheet_name1)
        sheet_name2 = utils.remove_invalid_characters(sheet_name2)

        if recap != None:
            if type(recap) == dict:
                recap = pd.DataFrame.from_dict(recap, orient="columns")
            recap.to_excel(self.writer, sheet_name="recap")
            self._recap = recap
        elif hasattr(self.source1.cfg, "recap"):
            if len(self._mismatch_src2) > 0 or len(self._mismatch_src1) > 0:
                self.source1.cfg.update_recap(
                    f"(!) WARNING (!) Count of mismatched",
                    len(self.mismatch_src1),
                    len(self.mismatch_src2),
                )
            recap = pd.DataFrame.from_dict(self.source1.cfg.recap, orient="columns")
            recap.to_excel(self.writer, sheet_name="recap")
            self._recap = recap

        self.mapper.full_map_report.to_excel(
            self.writer, sheet_name=sheet_name, index=False
        )

        if len(self._mismatch_src2) > 0:
            self._mismatch_src2.to_excel(
                self.writer, sheet_name=sheet_name2, index=False
            )
        if len(self._mismatch_src1) > 0:
            self._mismatch_src1.to_excel(
                self.writer, sheet_name=sheet_name1, index=False
            )
        if close_writer:
            self.writer.close()

    def _build_mismatch_usefullid(self, export_missingmap=True):
        self._mismatch_src1 = self.source1.initdf[
            ~self.source1.initdf[self.source1.cfg.source1["id_field"]].isin(
                self.source1._full_id_map[
                    self.source1.cfg.cfg["id_mapping"]["source1_id"]
                ]
            )
        ]
        self._mismatch_src2 = self.source2.initdf[
            ~self.source2.initdf[self.source2.cfg.source2["id_field"]].isin(
                self.source2._full_id_map[
                    self.source2.cfg.cfg["id_mapping"]["source2_id"]
                ]
            )
        ]

        self._missing_map_s1 = self.mapper.mapped_source1[
            ~self.mapper.mapped_source1[
                self.mapper.prefixcol_s1[self.source1.pivot]
            ].isin(self.match[self.source1.prefix + self.source1.pivot])
        ]
        self._missing_map_s2 = self.mapper.mapped_source2[
            ~self.mapper.mapped_source2[
                self.mapper.prefixcol_s2[self.source2.pivot]
            ].isin(self.match[self.source2.prefix + self.source2.pivot])
        ]

        if export_missingmap:
            self._missing_map_s1.to_excel(
                self.writer, sheet_name=f"mismaped_{self.source1.prefix}", index=False
            )
            self._missing_map_s2.to_excel(
                self.writer, sheet_name=f"mismaped_{self.source2.prefix}", index=False
            )

    @property
    def mapper(self):
        return self._mapper

    @property
    def source1(self):
        return self._source1

    @property
    def source2(self):
        return self._source2

    @property
    def match(self):
        return self._match

    @property
    def comp_table(self):
        return self._comp_table

    @property
    def desc(self):
        return self._desc

    @property
    def metrics(self):
        return self.desc.loc[["count", "match", "ratio"]]

    @property
    def excel_filename(self):
        if hasattr(self, "_excel_filename"):
            return self._excel_filename
        else:
            return None

    @property
    def id_mapping(self):
        return self._id_mapping

    @property
    def workbook(self):
        return self._workbook

    @property
    def writer(self):
        return self._writer

    @property
    def mismatch_src1(self):
        return self._mismatch_src1

    @property
    def mismatch_src2(self):
        return self._mismatch_src2

    @property
    def recap(self):
        return self._recap

    @property
    def exportpathdir(self):
        return self._exportpathdir
