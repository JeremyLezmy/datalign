#!/usr/bin/env python
# coding: utf-8

import re
import time
from merger import *
from utils import *


class BaseAlignment:
    def __init__(
        self,
        src_path,
        dest_path,
        mapping_path,
        main_id_src,
        main_id_dest,
        configpath="config.yml",
        pf_src=[],
        pf_dest=[],
        pf_alias=None,
        date_format_src="%Y/%m/%d",
        date_format_dest="%Y/%m/%d",
        src_name="sap",
        dest_name="dest",
        src_skiprows=2,
        dest_skiprows=0,
        src_file_sep=",",
        dest_file_sep=",",
        mapping_sep=";",
        is_case_sensitive=False,
    ):
        self.src_path = src_path
        self.dest_path = dest_path
        self.mapping_path = mapping_path

        self.is_case_sensitive = is_case_sensitive

        mapp = custom_read_df(
            mapping_path,
            sep=mapping_sep,
            skiprows=0,
            lowerall=not self.is_case_sensitive,
        )
        self.mapp = self.apply_mapping_suffix(mapp.copy(), path=self.mapping_path)

        self.src_folder = os.path.dirname(self.src_path) + "/"
        self.dest_folder = os.path.dirname(self.dest_path) + "/"

        self.src_date = (
            self.src_path.rsplit(".")[-2][-8:]
            if self.src_path.rsplit(".")[-2][-8:].isnumeric()
            else ""
        )
        self.dest_date = (
            self.dest_path.rsplit(".")[-2][-8:]
            if self.dest_path.rsplit(".")[-2][-8:].isnumeric()
            else ""
        )

        self.main_id_src = (
            main_id_src if self.is_case_sensitive else main_id_src.lower()
        )
        self.main_id_dest = (
            main_id_dest if self.is_case_sensitive else main_id_dest.lower()
        )

        self.src_name = src_name
        self.dest_name = dest_name

        self.configpath = configpath

        self.pf_src = pf_src if self.is_case_sensitive else [k.lower() for k in pf_src]
        self.pf_dest = (
            pf_dest if self.is_case_sensitive else [k.lower() for k in pf_dest]
        )
        if pf_alias is not None:
            self.pf_alias = pf_alias
        else:
            self.pf_alias = [k[15] if len(k) >= 15 else k for k in pf_src]

        self.date_format = {"src": date_format_src, "dest": date_format_dest}
        self.missing_col = {}

        self.initdf_src = custom_read_df(
            self.src_path,
            skiprows=int(src_skiprows),
            sep=src_file_sep,
            lowerall=not self.is_case_sensitive,
        )
        self.initdf_dest = custom_read_df(
            self.dest_path,
            skiprows=int(dest_skiprows),
            sep=dest_file_sep,
            lowerall=not self.is_case_sensitive,
        )

        self.src_filterfields = None
        self.src_filter_values = None
        self.dest_filterfields = None
        self.dest_filter_values = None

        self.src_lbda_fields = None
        self.src_lbda_fct = None
        self.dest_lbda_fields = None
        self.dest_lbda_fct = None

        self.src_merge_fields = None
        self.src_merge_fields_res = None
        self.dest_merge_fields = None
        self.dest_merge_fields_res = None

        self.src_automerge = None
        self.dest_automerge = None

        self.src_priority_field = None
        self.src_priority_value = None
        self.dest_priority_field = None
        self.dest_priority_value = None

    @classmethod
    def from_config_file(cls, configpath):
        cfgt = Config.from_filename(configpath)
        keys_id = ["method", "pivot_alias", "case_sensitive"]
        method, pf_alias, is_case_sensitive = [
            cfgt.cfg["id_mapping"].get(k) for k in keys_id
        ]

        keys_map = ["separator", "init_path"]
        mapping_sep, mapping_path = [cfgt.cfg["mappingfile"].get(k) for k in keys_map]

        keys_src = [
            "fields_to_merge",
            "fields_to_merge_name",
            "dupl_priority_field",
            "dupl_priority_value",
            "fields_to_filter",
            "values_to_filter",
            "automerge_field",
            "fields_lbda_trans",
            "lbda_trans",
            "all_pivots",
            "date_format",
            "separator",
            "skiprows",
            "main_id",
            "mapping_col_name",
            "name",
            "init_path",
        ]
        (
            fields_to_merge_src,
            fields_to_merge_name_src,
            dupl_priority_field_src,
            dupl_priority_value_src,
            fields_to_filter_src,
            values_to_filter_src,
            automerge_field_src,
            fields_lbda_src,
            lbda_trans_src,
            pf_src,
            date_format_src,
            src_file_sep,
            src_skiprows,
            main_id_src,
            mapping_col_name_src,
            src_name,
            src_path,
        ) = [cfgt.cfg["source1"].get(k) for k in keys_src]

        keys_dest = [
            "fields_to_merge",
            "fields_to_merge_name",
            "dupl_priority_field",
            "dupl_priority_value",
            "fields_to_filter",
            "values_to_filter",
            "automerge_field",
            "fields_lbda_trans",
            "lbda_trans",
            "all_pivots",
            "date_format",
            "separator",
            "skiprows",
            "main_id",
            "mapping_col_name",
            "name",
            "init_path",
        ]
        (
            fields_to_merge_dest,
            fields_to_merge_name_dest,
            dupl_priority_field_dest,
            dupl_priority_value_dest,
            fields_to_filter_dest,
            values_to_filter_dest,
            automerge_field_dest,
            fields_lbda_dest,
            lbda_trans_dest,
            pf_dest,
            date_format_dest,
            dest_file_sep,
            dest_skiprows,
            main_id_dest,
            mapping_col_name_dest,
            dest_name,
            dest_path,
        ) = [cfgt.cfg["source2"].get(k) for k in keys_dest]

        this = cls(
            src_path,
            dest_path,
            mapping_path,
            main_id_src,
            main_id_dest,
            configpath,
            pf_src,
            pf_dest,
            pf_alias,
            date_format_src,
            date_format_dest,
            src_name,
            dest_name,
            src_skiprows,
            dest_skiprows,
            src_file_sep,
            dest_file_sep,
            mapping_sep,
            is_case_sensitive,
        )

        if not is_case_sensitive:
            fields_to_filter_src = recursive_lower(fields_to_filter_src)
            fields_to_filter_dest = recursive_lower(fields_to_filter_dest)
            values_to_filter_src = recursive_lower(values_to_filter_src)
            values_to_filter_dest = recursive_lower(values_to_filter_dest)
            fields_lbda_dest = recursive_lower(fields_lbda_dest)
            fields_lbda_src = recursive_lower(fields_lbda_src)
            fields_to_merge_dest = recursive_lower(fields_to_merge_name_dest)
            fields_to_merge_src = recursive_lower(fields_to_merge_src)
            fields_to_merge_name_src = recursive_lower(fields_to_merge_name_src)
            fields_to_merge_name_dest = recursive_lower(fields_to_merge_name_dest)
            dupl_priority_field_src = recursive_lower(dupl_priority_field_src)
            dupl_priority_value_src = recursive_lower(dupl_priority_value_src)
            dupl_priority_field_dest = recursive_lower(dupl_priority_field_dest)
            dupl_priority_value_dest = recursive_lower(dupl_priority_value_dest)
            automerge_field_src = recursive_lower(automerge_field_src)
            automerge_field_dest = recursive_lower(automerge_field_dest)

        this.set_filtering("src", fields_to_filter_src, values_to_filter_src)
        this.set_filtering("dest", fields_to_filter_dest, values_to_filter_dest)

        this.set_lbda_transformations("src", fields_lbda_src, lbda_trans_src)
        this.set_lbda_transformations("dest", fields_lbda_dest, lbda_trans_dest)

        this.set_fields_to_merge("src", fields_to_merge_src, fields_to_merge_name_src)
        this.set_fields_to_merge(
            "dest", fields_to_merge_dest, fields_to_merge_name_dest
        )

        this.set_automerge("src", automerge_field_src)
        this.set_automerge("dest", automerge_field_dest)

        this.set_duplpriority_field(
            "src", dupl_priority_field_src, dupl_priority_value_src
        )
        this.set_duplpriority_field(
            "dest", dupl_priority_field_dest, dupl_priority_value_dest
        )

        return this

    def set_automerge(self, kind, fields):
        if kind == "src":
            self.src_automerge = fields
        if kind == "dest":
            self.dest_automerge = fields

    def set_filtering(self, kind, fields, values):
        if kind == "src":
            self.src_filterfields = fields
            self.src_filter_values = values
        if kind == "dest":
            self.dest_filterfields = fields
            self.dest_filter_values = values

    def set_lbda_transformations(self, kind, fields, lbda):
        if kind == "src":
            self.src_lbda_fields = fields
            self.src_lbda_fct = lbda
        if kind == "dest":
            self.dest_lbda_fields = fields
            self.dest_lbda_fct = lbda

    def set_fields_to_merge(self, kind, fields, destfields):
        if kind == "src":
            self.src_merge_fields = fields
            self.src_merge_fields_res = destfields

        if kind == "dest":
            self.dest_merge_fields = fields
            self.dest_merge_fields_res = destfields

    def set_duplpriority_field(self, kind, field, value):
        if kind == "src":
            self.src_priority_field = field
            self.src_priority_value = value

        if kind == "dest":
            self.dest_priority_field = field
            self.dest_priority_value = value

    def apply_duplicate_ghost_mapping(self, df, key):
        for column in self.mapp[key]:
            match = re.match(r"^(.+)\.\d+$", column)
            if match:
                base_column = match.group(1)
                if base_column in df.columns:
                    df[column] = df[base_column]
            if "virtual field" in column:
                df[column] = [""] * len(df)

        return df

    @staticmethod
    def apply_mapping_suffix(df, path=None):
        newmapp = df.copy()
        virtual_field_count = 0
        for column in ["src", "dest"]:
            count_dict = {}
            for index, value in newmapp[column].items():
                if value in [None, ""]:
                    virtual_field_count += 1
                    newmapp.at[index, column] = f"virtual field.{virtual_field_count}"
                elif value in count_dict:
                    count_dict[value] += 1
                    newmapp.at[index, column] = f"{value}.{count_dict[value]-1}"
                else:
                    count_dict[value] = 1

        if path != None:
            newmapp.to_csv(path, index=False, sep=";")
        return newmapp

    def process(self):
        if os.path.isdir(os.path.join(self.dest_folder, "transformations")):
            transpath_dest = self.dest_folder
        else:
            transpath_dest = None
        print(
            f"Transformation folder : {os.path.join(self.dest_folder, 'transformations')}"
        )

        self.procdf_src = self._base_process(
            self.initdf_src.copy(), key="src", transpath=transpath_dest
        )
        self.procdf_src, self.warning_dupl_src, self.no_mainid_src = self._get_warning(
            self.procdf_src,
            self.main_id_src,
            self.src_priority_field,
            self.src_priority_value,
        )
        self.procdf_src = self.apply_automerge(self.procdf_src.copy(), "src")

        self.procdf_dest = self._base_process(
            self.initdf_dest.copy(), key="dest", transpath=transpath_dest
        )
        (
            self.procdf_dest,
            self.warning_dupl_dest,
            self.no_mainid_dest,
        ) = self._get_warning(
            self.procdf_dest,
            self.main_id_dest,
            self.dest_priority_field,
            self.dest_priority_value,
        )
        self.procdf_dest = self.apply_automerge(self.procdf_dest.copy(), "dest")

    def _base_process(self, df, key="src", transpath=None):
        if key == "src" and "src" not in self.mapp.columns:
            key == "payroll"

        df = self.apply_duplicate_ghost_mapping(df.copy(), key)
        df = self.apply_filter(df.copy(), key)
        df = self.apply_lbda_transformation(df.copy(), key)
        df = self.apply_fields_merge(df.copy(), key)

        self.missing_col[key] = [c for c in self.mapp[key] if c not in df.columns]
        df = df[[c for c in self.mapp[key] if c in df.columns]]

        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.replace(np.nan, "", regex=True, inplace=True)

        df_datefield = [
            c
            for c, d in self.mapp[[key, "type"]].itertuples(index=False, name=None)
            if d == "date" and c in df.columns
        ]
        print(f"Parsing date for key {key} with format: {self.date_format[key]}")
        print(df[df_datefield].head())
        for c in df_datefield:
            print(f"Date Field : {c}")
            df[c].replace("", np.nan, inplace=True)
            df[c] = df[c].apply(
                lambda x: pd.to_datetime(
                    x, format=self.date_format[key], errors="ignore"
                )
            )
            df[c] = df[c].apply(
                lambda x: pd.to_datetime(x, format="%Y/%m/%d", errors="ignore")
            )

            df[c].replace([pd.NaT, "NaT", np.nan, "NaN"], "", inplace=True)
            df[c] = df[c].apply(
                lambda x: str(x).split(" ")[0] if " " in str(x) else str(x)
            )
            df[c].replace([pd.NaT, "NaT", np.nan, "NaN"], "", inplace=True)
        print(df[df_datefield].head())
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)

        if transpath != None:
            df = get_transformations(
                df.copy(), transpath, lowerall=not self.is_case_sensitive
            )

        return df

    def apply_filter(self, df, key):
        if (
            key == "src"
            and isinstance(self.src_filterfields, list)
            and is_list_of_lists(self.src_filter_values)
        ):
            for i, field in enumerate(self.src_filterfields):
                # print(f"field : {field}")
                df = df[df[field].isin(self.src_filter_values[i])]
        if (
            key == "dest"
            and isinstance(self.dest_filterfields, list)
            and is_list_of_lists(self.dest_filter_values)
        ):
            # print(f"field : {field}")
            for i, field in enumerate(self.dest_filterfields):
                df = df[df[field].isin(self.dest_filter_values[i])]
        return df

    def apply_fields_merge(self, df, key):
        if (
            key == "src"
            and is_list_of_lists(self.src_merge_fields)
            and isinstance(self.src_merge_fields_res, list)
        ):
            for i, field in enumerate(self.src_merge_fields):
                print(f"Merging fields for src : {field}")
                df[self.src_merge_fields_res[i]] = df[field].apply(
                    lambda row: " ".join(
                        [
                            str(value)
                            for value in row.values
                            if value is not None and value != ""
                        ]
                    ),
                    axis=1,
                )
        if (
            key == "dest"
            and is_list_of_lists(self.dest_merge_fields)
            and isinstance(self.dest_merge_fields_res, list)
        ):
            for i, field in enumerate(self.dest_merge_fields):
                print(f"Merging fields for dest: {field}")
                df[self.dest_merge_fields_res[i]] = df[field].apply(
                    lambda row: " ".join(
                        [
                            str(value)
                            for value in row.values
                            if value is not None and value != ""
                        ]
                    ),
                    axis=1,
                )
        return df

    def apply_automerge(self, df, key):
        if key == "src":
            fields_to_automerge = self.src_automerge
        elif key == "dest":
            fields_to_automerge = self.dest_automerge

        print(f"Length DF {key}:{len(df)}")
        if is_list_of_lists(fields_to_automerge):
            for field in fields_to_automerge:
                if len(field) in [3, 4]:
                    print(f"Automerging fields for {key} : {field}")

                    df_sub = df[[field[1], field[2]]]
                    df_sub.rename(
                        columns={
                            field[1]: "Field to change",
                            field[2]: "Merging pivot",
                        },
                        inplace=True,
                    )
                    df_sub.drop_duplicates(inplace=True)
                    df_sub.reset_index(inplace=True, drop=True)
                    try:
                        new_values = df.merge(
                            df_sub,
                            left_on=field[0],
                            right_on="Merging pivot",
                            how="left",
                            validate="many_to_one",
                        )["Field to change"]
                    except pd.errors.MergeError:
                        import warnings

                        warnings.warn(
                            f"MergeError: Merge keys are not unique in right dataset; not a many-to-one merge. Automerge for {field[0]} ignored",
                            stacklevel=2,
                        )
                    if len(field) == 3:
                        destfield = field[0]
                    elif len(field) == 4:
                        destfield = field[3]
                    print(new_values)
                    df[destfield] = new_values.values
        print(f"New Length DF {key}:{len(df)}")
        return df

    def apply_lbda_transformation(self, df, key):
        if (
            key == "src"
            and isinstance(self.src_lbda_fields, list)
            and isinstance(self.src_lbda_fct, list)
        ):
            for field, fct in zip(self.src_lbda_fields, self.src_lbda_fct):
                print(f"Applying lbda transformation for src on field : {field}")
                fct = fct.lower() if not self.is_case_sensitive else fct
                df[field] = df.apply(eval(fct), axis=1, result_type="expand")

        if (
            key == "dest"
            and isinstance(self.dest_lbda_fields, list)
            and isinstance(self.dest_lbda_fct, list)
        ):
            for field, fct in zip(self.dest_lbda_fields, self.dest_lbda_fct):
                print(f"Applying lbda transformation for dest on field : {field}")
                print(df.head())
                fct = fct.lower() if not self.is_case_sensitive else fct
                df[field] = df.apply(eval(fct), axis=1, result_type="expand")
        return df

    def _get_warning(self, df, main_id, priority_field=None, priority_value=None):
        no_mainid = df[df[main_id] == ""]
        idx = no_mainid.index
        df = df.query("index not in @idx")

        if priority_field in df.columns and priority_value in df[priority_field].values:
            warning_dupl = df[df.duplicated(main_id, keep=False)]
            unique_ids = warning_dupl[main_id].unique()
            grouped = [
                warning_dupl[warning_dupl[main_id] == unid] for unid in unique_ids
            ]
            idx_torem = []
            for group in grouped:
                if (
                    len(group) > 1
                    and len(group[priority_field].unique()) > 1
                    and priority_value in group[priority_field].values
                    and len(group[group[priority_field] == priority_value]) == 1
                ):
                    print(group[group[priority_field] == priority_value].index)
                    idx_torem.append(
                        group[group[priority_field] == priority_value].index.values[0]
                    )
                    print(group[group[priority_field] == priority_value][main_id])
            idx = pd.Index(idx_torem)
            warning_dupl = warning_dupl.query("index not in @idx")
            df = df[~df.index.isin(warning_dupl.index)]
            warning_dupl.replace(["NaN", np.nan], "", inplace=True, regex=True)
        else:
            warning_dupl = df[df.duplicated(main_id, keep=False)]
            df = df[~df.index.isin(warning_dupl.index)]
            warning_dupl.replace(["NaN", np.nan], "", inplace=True, regex=True)

        return df, warning_dupl, no_mainid

    @staticmethod
    def run_alignment(
        src_path,
        dest_path,
        mapping_path,
        pivots_src,
        pivots_dest,
        main_id_src,
        main_id_dest,
        configpath,
        working_dir=None,
        src_name="src",
        dest_name="dest",
        pivots_alias=None,
        update_recap=None,
        filename=None,
        return_sources=True,
        close_writer=True,
        logger=None,
        progress=None,
    ):
        if progress != None:
            p = progress[-1]
        cfg = Config.from_filename(configpath)

        cfg.set_value(key="id_mapping", **{"source2_id": main_id_dest.lower()})
        cfg.set_value(key="options", **{"use_full_id_map": False})
        cfg.set_value(key="mappingfile", **{"filename": mapping_path})
        cfg.set_value(
            key="source1",
            **{
                "filename": src_path,
                "get_contry_from_le": False,
                "id_field": main_id_src.lower(),
            },
        )
        cfg.set_value(
            key="source2",
            **{
                "prefix": dest_name + "_",
                "mapping_col_name": "dest",
                "filename": dest_path,
                "id_field": main_id_dest.lower(),
                "country_field": "",
            },
        )

        dest_folder = os.path.dirname(dest_path) + "/"

        if working_dir is None:
            working_dir = os.path.abspath(
                dest_folder + f"/export_src_" + time.strftime("%Y%m%d_%H%M%S")
            )

        writer = None
        if pivots_alias is None:
            pivots_alias = [k[15] if len(k) >= 15 else k for k in pivots_src]

        for pfs, pf1, pf2 in zip(pivots_alias, pivots_src, pivots_dest):
            source1 = SourceDF.from_path(src_path, cfg, source="source1")
            source2 = SourceDF.from_path(dest_path, cfg, source="source2")
            print(f"source1:{src_path}")
            print(f"source2:{dest_path}")
            source1.cfg.create_recap(overwrite=True)
            source1.cfg.update_recap(
                "Initial count of records (warnings excluded)",
                len(source1.df.drop_duplicates(main_id_src, keep=False)),
                len(source2.df.drop_duplicates(main_id_dest, keep=False)),
            )

            source1.cfg.set_value(key="source1", **{"pivot_field": pf1})
            source2.cfg.set_value(key="source2", **{"pivot_field": pf2})

            if source1.cfg.debug:
                print(f"Pre-processing files with pivots :{pf1, pf2}")
                if logger != None:
                    logger.append(f"Pre-processing files with pivots :{pf1, pf2}")

            source1.preprocess()
            print(source2.df)
            source2.preprocess()
            source1.cfg.update_recap(
                f"Count of unmatched (Null) {source2.pivot}",
                len(source1._nullpivot),
                len(source2._nullpivot),
            )
            source1.cfg.update_recap(
                f"Count of duplicated {source2.pivot} (multiple mapping)",
                source1._lenuniqueduplicate,
                source2._lenuniqueduplicate,
            )
            source1.cfg.update_recap(
                f"Count of unique {source2.pivot}", len(source1.df), len(source2.df)
            )

            mapper = Mapper.from_path(mapping_path, source1, source2)
            mapper.preprocess(apply_mapping=True)
            # mapper.parse_dates()

            merger = Merger(mapper, exportpathdir=working_dir)
            merger.merge_sources()

            if "id_key" in pivots_src:
                matches = merger.match.copy()

                multiple_mapping_src = matches[
                    matches.duplicated(source1.prefix + main_id_src, keep=False)
                ].sort_values(source1.prefix + main_id_src)

                multiple_mapping_dest = matches[
                    matches.duplicated(source2.prefix + main_id_dest, keep=False)
                ].sort_values(source2.prefix + main_id_dest)

                multiple_mapping_src.replace(
                    ["NaN", np.nan], "", inplace=True, regex=True
                )
                multiple_mapping_dest.replace(
                    ["NaN", np.nan], "", inplace=True, regex=True
                )

                matches = matches[
                    ~matches.index.isin(
                        pd.Index.union(
                            multiple_mapping_src.index, multiple_mapping_dest.index
                        )
                    )
                ]

                merger.mapper._mappedsource2 = merger.mapper._mappedsource2[
                    ~merger.mapper._mappedsource2[source2.prefix + "id_key"].isin(
                        pd.concat([multiple_mapping_dest, multiple_mapping_src])[
                            source2.prefix + "id_key"
                        ]
                    )
                ]

                merger.mapper._mappedsource1 = merger.mapper._mappedsource1[
                    ~merger.mapper._mappedsource1[source1.prefix + "id_key"].isin(
                        pd.concat([multiple_mapping_dest, multiple_mapping_src])[
                            source1.prefix + "id_key"
                        ]
                    )
                ]

                merger._match = matches

            merger.source1.cfg.update_recap(
                f"Matched {source2.pivot}", len(merger.match), len(merger.match)
            )

            if update_recap != None:
                source1.cfg.update_recap(None, None, None)
                for vals in update_recap:
                    source1.cfg.update_recap(*vals)
                source1.cfg.update_recap(None, None, None)

            if cfg.cfg["options"]["use_full_id_map"]:
                for source, source_df in merger.match.groupby(
                    source1.prefix + "source"
                ):
                    merger.source1.cfg.update_recap(
                        f"from :{source}", len(source_df), len(source_df)
                    )

            merger.process_basic_analysis()
            merger.build_excel_filename(excel_name=filename)
            merger.export_pivot_field_matching()
            merger.write_excel(writer=writer, pivot_alias=pfs)

            writer = merger.writer

            if len(source1._duplicatepivots) > 0:
                source1._duplicatepivots.to_excel(
                    writer,
                    sheet_name=f"{source1.prefix}" + f"{pfs}_duplicate",
                    index=False,
                )
            if len(source2._duplicatepivots) > 0:
                source2._duplicatepivots.to_excel(
                    writer,
                    sheet_name=f"{source2.prefix}" + f"{pfs}_duplicate",
                    index=False,
                )
            if len(source1._nullpivot) > 0:
                source1._nullpivot.to_excel(
                    writer, sheet_name=f"{source1.prefix}" + f"{pfs}_null", index=False
                )
            if len(source2._nullpivot) > 0:
                source2._nullpivot.to_excel(
                    writer, sheet_name=f"{source2.prefix}" + f"{pfs}_null", index=False
                )

            if "id_key" in pivots_src:
                merger.export_mismatch()

            if "id_key" in pivots_src:
                if len(multiple_mapping_src) > 0 or len(multiple_mapping_dest) > 0:
                    multiple_match = pd.concat(
                        [multiple_mapping_dest, multiple_mapping_src]
                    ).sort_values(source1.prefix + "id_key")
                    multiple_match.drop_duplicates(inplace=True)
                    multiple_match.to_excel(
                        writer,
                        sheet_name=f"WARNING_multiple_match",
                        index=False,
                    )
                    apply_comparison_formatting(
                        multiple_match,
                        writer,
                        f"WARNING_multiple_match",
                    )
                    source1.cfg.update_recap(
                        f"(!) WARNING (!) Count of multiple match (Total)",
                        len(multiple_match),
                        len(multiple_match),
                    )

                recap = pd.DataFrame.from_dict(source1.cfg.recap, orient="columns")
                recap.to_excel(writer, sheet_name="recap")

            if progress != None:
                p += (0.6 - 0.2) / len(pivots_src)
                progress.append(round(p, 2))
                print(progress[-1])

        if close_writer:
            writer.close()
        if return_sources:
            return source1, source2, working_dir, writer
