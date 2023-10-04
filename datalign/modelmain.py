from base import *
from time import sleep
import sys


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


class Model:
    def __init__(self, mode="Basic"):
        self.loaded_files = [None, None, None]
        self.src_args = {}
        self.dest_args = {}
        self.log = []
        self.progress = [0]
        self.mode = mode

    @classmethod
    def run_from_config_file(cls, configpath):
        cfgt = Config.from_filename(configpath)

        method = cfgt.cfg["id_mapping"].get("method")
        this = cls(mode=method)
        if method == "Basic":
            # this.log.append(f"Setting Config file {configpath}")
            alignmode = BaseAlignment.from_config_file(configpath=configpath)
            return this.process_data(alignmode=alignmode)
        else:
            raise NotImplementedError(
                f"{method} not implemented. Only Basic method available."
            )

    @classmethod
    def cls_from_config_file(cls, configpath):
        cfgt = Config.from_filename(configpath)

        method = cfgt.cfg["id_mapping"].get("method")
        this = cls(mode=method)
        if method == "Basic":
            # this.log.append(f"Setting Config file {configpath}")
            this.alignobj = BaseAlignment.from_config_file(configpath=configpath)
            return this
        else:
            raise NotImplementedError(
                f"{method} not implemented. Only Basic method available."
            )

    def set_file(self, index, file_path):
        self.loaded_files[index] = file_path

    def set_mode(self, mode):
        self.mode = mode

    def set_config_file(self, file_path):
        self.config_file = file_path

    def set_src_args(self, **kwargs):
        self.log.append("Setting src arguments")
        for key, val in kwargs.items():
            self.src_args[key] = val
            self.log.append(f"{key} --> {val}")
        self.log.append("#" * 50)
        self.progress.append(0.01)

    def set_dest_args(self, **kwargs):
        self.log.append("Setting dest arguments")
        for key, val in kwargs.items():
            self.dest_args[key] = val
            self.log.append(f"{key} --> {val}")
        self.log.append("#" * 50)
        self.progress.append(0.02)

    def get_dest_args(self):
        return self.dest_args

    def get_src_args(self):
        return self.src_args

    def get_files(self):
        return self.loaded_files

    def remove_source_file(self):
        self.loaded_files[0] = None

    def remove_dest_file(self):
        self.loaded_files[1] = None

    def remove_mapping_file(self):
        self.loaded_files[2] = None

    def _clear_logs(self):
        self.log = []
        self.progress = [0]

    def process_data(self, alignmode=None):
        if alignmode is None:
            self.log.append("Setting files.")
            src_path, dest_path, mapping_path = self.get_files()

            (
                src_name,
                main_id_src,
                pf_src,
                date_format_src,
                skiprow_src,
                sep_src,
                pf_alias,
            ) = self.get_src_args().values()

            (
                dest_name,
                main_id_dest,
                pf_dest,
                date_format_dest,
                skiprow_dest,
                sep_dest,
            ) = self.get_dest_args().values()
            self.progress.append(0.03)

            if self.mode == "Basic":
                if not os.path.exists(resource_path("config.yml")):
                    self.generate_config_file(resource_path("config.yml"))
                alignmode = BaseAlignment(
                    src_path,
                    dest_path,
                    mapping_path,
                    main_id_src,
                    main_id_dest,
                    resource_path("config.yml"),
                    pf_src,
                    pf_dest,
                    pf_alias,
                    date_format_src,
                    date_format_dest,
                    src_name,
                    dest_name,
                    skiprow_src,
                    skiprow_dest,
                    sep_src,
                    sep_dest,
                )

            else:
                self.progress.append(1)
                self.log.append(f"Test done")
                sleep(0.3)
                return "test"

        self.log.append("Processing src and dest files ...")
        self.progress.append(0.1)
        sleep(1)
        alignmode.process()
        self.progress.append(0.15)
        self.log.append("Processing done.")
        # print(f"src df:  {alignmode.procdf_src.head(2)}")
        # print(f"dest df:  {alignmode.procdf_dest.head(2)}")

        src_df = alignmode.procdf_src
        dest_df = alignmode.procdf_dest

        warning_dupl_src = alignmode.warning_dupl_src
        warning_dupl_dest = alignmode.warning_dupl_dest
        no_mainid_dest = alignmode.no_mainid_dest
        no_mainid_src = alignmode.no_mainid_src

        self.log.append("Saving processed files...")
        src_df.to_csv(
            alignmode.src_folder + f"{alignmode.src_name}_{alignmode.src_date}.csv",
            sep=";",
            index=False,
        )
        dest_df.to_csv(
            alignmode.dest_folder + f"{alignmode.dest_name}_{alignmode.dest_date}.csv",
            sep=";",
            index=False,
        )
        self.progress.append(0.2)
        self.log.append(
            f"Processed files saved in this folder : {alignmode.dest_folder}."
        )
        self.log.append("#" * 50)

        src_path = (
            alignmode.src_folder + f"{alignmode.src_name}_{alignmode.src_date}.csv"
        )
        mapping_path = alignmode.mapping_path
        dest_path = (
            alignmode.dest_folder + f"{alignmode.dest_name}_{alignmode.dest_date}.csv"
        )

        self.log.append(f"Running alignment on pivots.")

        source1, source2, working_dir, _ = alignmode.run_alignment(
            src_path=src_path,
            dest_path=dest_path,
            mapping_path=mapping_path,
            pivots_src=alignmode.pf_src,
            pivots_dest=alignmode.pf_dest,
            main_id_src=alignmode.main_id_src,
            main_id_dest=alignmode.main_id_dest,
            configpath=alignmode.configpath,
            working_dir=None,
            src_name=alignmode.src_name,
            dest_name=alignmode.dest_name,
            pivots_alias=alignmode.pf_alias,
            return_sources=True,
            logger=self.log,
            progress=self.progress,
        )
        self.progress.append(0.6)
        self.log.append(f"Alignment on pivots done.")
        self.log.append("#" * 50)
        sleep(0.3)

        self.log.append("#" * 50)
        self.log.append(f"Merging pivots matches...")

        idpivot = [alignmode.main_id_src.lower(), alignmode.main_id_dest.lower()]

        multidf = [
            pd.read_csv(
                os.path.join(working_dir, f"{pf}_match.csv"),
                sep=";",
            )
            for pf in alignmode.pf_dest
        ]
        full_match = pd.concat(multidf)
        multidf = [
            pd.read_csv(
                os.path.join(working_dir, f"{pf}_match.csv"),
                sep=";",
                dtype={k: str for k in full_match.columns},
            )
            for pf in alignmode.pf_dest
        ]
        full_match = pd.concat(multidf)

        rename_dict = {
            alignmode.src_name
            + "_"
            + alignmode.main_id_src.lower(): alignmode.src_name
            + " main id"
        } | {
            c: c.replace(source2.prefix, "")
            for c in full_match.columns
            if source2.prefix in c
        }
        full_match = full_match.rename(columns=rename_dict)

        full_match["id_key"] = (
            full_match[alignmode.src_name + " main id"] + "-" + full_match[idpivot[1]]
        )

        full_match = full_match.drop_duplicates(subset=["id_key"], keep="first")

        # source1_id_in_map = source1.cfg.idmapping["source1_id"]
        source2_id_in_map = source1.cfg.source2["main_id"].lower()
        print(source2_id_in_map)
        print(alignmode.main_id_dest.lower())
        full_match = full_match[
            [
                alignmode.src_name + " main id",
                alignmode.main_id_dest.lower(),
                "id_key",
                alignmode.pf_dest[0],
                "matching_source",
            ]
        ]
        full_id_map = full_match.copy()
        self.progress.append(0.7)
        sleep(0.1)
        self.log.append(f"Pivot matches merged.")

        df2 = dest_df.copy()
        df2.replace(np.nan, "", regex=True, inplace=True)
        df2.columns = df2.columns.str.lower()
        df2 = df2.apply(lambda x: x.astype(str).str.lower())
        df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df2.drop_duplicates(inplace=True)

        df1 = src_df.copy()
        df1.replace(np.nan, "", regex=True, inplace=True)
        df1.columns = df1.columns.str.lower()
        df1 = df1.apply(lambda x: x.astype(str).str.lower())
        df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df1.drop_duplicates(inplace=True)

        full_id_map = full_id_map.loc[:, ~full_id_map.columns.duplicated()].copy()

        full_id_map.to_csv(
            os.path.join(working_dir, "full_id_map.csv"), sep=";", index=False
        )

        s1 = df1.merge(
            full_id_map,
            left_on=alignmode.main_id_src.lower(),
            right_on=alignmode.src_name + " main id",
            suffixes=(None, "_x"),
            how="left",
        )
        s2 = df2.merge(
            full_id_map,
            left_on=alignmode.main_id_dest.lower(),
            right_on=alignmode.main_id_dest.lower(),
            suffixes=(None, "_x"),
            how="left",
        )

        s1_full = s1[
            [
                c
                for c in s1.columns
                if c[-2:] != "_x"
                and c
                not in [
                    "country",
                    alignmode.main_id_dest.lower(),
                    alignmode.src_name + " main id",
                ]
            ]
        ]
        s2_full = s2[
            [
                c
                for c in s2.columns
                if c[-2:] != "_x" and c not in [alignmode.src_name + " main id"]
            ]
        ]

        s2_full[s2_full["id_key"].astype(str) != "nan"].shape, full_id_map.shape

        rename_dict = {
            c: c.replace(source2.prefix, "")
            for c in s2_full.columns
            if source2.prefix in c
        }
        s2_full = s2_full.rename(columns=rename_dict)

        rename_dict = {
            c: c.replace(source1.prefix, "")
            for c in s1_full.columns
            if source1.prefix in c
        }
        s1_full = s1_full.rename(columns=rename_dict)

        s1_full.replace(np.nan, "", regex=True, inplace=True)
        s2_full.replace(np.nan, "", regex=True, inplace=True)

        self.log.append(f"Saving intermediate files...")

        s2_full.to_csv(
            alignmode.dest_folder
            + f"{alignmode.dest_name}_{alignmode.dest_date}_full_idkey.csv",
            sep=";",
            index=False,
        )
        s1_full.to_csv(
            alignmode.src_folder
            + f"{alignmode.src_name}_{alignmode.src_date}_full_idkey.csv",
            sep=";",
            index=False,
        )

        map_fullid = alignmode.mapp.append(
            {"src": "id_key", "dest": "id_key"}, ignore_index=True
        )
        map_fullid = map_fullid.append(
            {"src": "matching_source", "dest": "matching_source"}, ignore_index=True
        )
        map_fullid.to_csv(
            alignmode.dest_folder + "mapping_full_idkey.csv", sep=";", index=False
        )
        self.progress.append(0.75)
        sleep(0.1)
        self.log.append(f"Saved.")
        self.log.append("#" * 50)

        src_path = (
            alignmode.src_folder
            + f"{alignmode.src_name}_{alignmode.src_date}_full_idkey.csv"
        )
        mapping_path = alignmode.dest_folder + "mapping_full_idkey.csv"
        dest_path = (
            alignmode.dest_folder
            + f"{alignmode.dest_name}_{alignmode.dest_date}_full_idkey.csv"
        )

        self.log.append(f"Running final alignment.")
        filename = (
            "full_fields_comparison_" + alignmode.dest_name + "_" + alignmode.dest_date
        )

        working_dir = os.path.abspath(working_dir + "full_id_map")

        update_recap = [
            [
                "(!) WARNING (!) Count of DUPLICATED MAIN ID",
                len(warning_dupl_src),
                len(warning_dupl_dest),
            ],
            [
                "(!) WARNING (!) Count of NO MAIN ID",
                len(no_mainid_src),
                len(no_mainid_dest),
            ],
        ]

        source1, source2, working_dir, writer = alignmode.run_alignment(
            src_path=src_path,
            dest_path=dest_path,
            mapping_path=mapping_path,
            pivots_src=["id_key"],
            pivots_dest=["id_key"],
            main_id_src=alignmode.main_id_src,
            main_id_dest=alignmode.main_id_dest,
            configpath=alignmode.configpath,
            working_dir=working_dir,
            src_name=alignmode.src_name,
            dest_name=alignmode.dest_name,
            pivots_alias=None,
            update_recap=update_recap,
            return_sources=True,
            close_writer=False,
            logger=self.log,
            filename=filename,
        )
        self.progress.append(0.9)
        sleep(0.1)
        self.log.append(f"Alignment on pivots done.")
        self.log.append("#" * 50)

        self.log.append(f"Saving anomalies (duplicates + no main id)...")

        if len(no_mainid_dest) > 0:
            no_mainid_dest.to_excel(
                writer,
                sheet_name=f"{source2.prefix}" + f"WARNING_no_mainid",
                index=False,
            )
        if len(no_mainid_src) > 0:
            no_mainid_src.to_excel(
                writer,
                sheet_name=f"{source1.prefix}" + f"WARNING_no_mainid",
                index=False,
            )

        if len(warning_dupl_src) > 0:
            warning_dupl_src.to_excel(
                writer,
                sheet_name=f"{source1.prefix}" + f"WARNING_dupl_mainid",
                index=False,
            )

        if len(warning_dupl_dest) > 0:
            warning_dupl_dest.to_excel(
                writer,
                sheet_name=f"{source2.prefix}" + f"WARNING_dupl_mainid",
                index=False,
            )

        writer.close()
        self.progress.append(0.95)

        self.log.append(f"Process done!")
        sleep(0.2)
        self.log.append(f"Adjusting Excel file ...")
        excel_autoadjust_col(os.path.join(working_dir, filename + ".xlsx"))
        self.progress.append(1)
        self.log.append(f"Export ready!")
        sleep(1)
        return os.path.join(working_dir, filename + ".xlsx")

    def generate_config_file(self, file_path):
        import yaml

        config_data = {
            "id_mapping": {
                "case_sensitive": False,
                "filename": "id_mapping.csv",
                "method": "Basic",
                "pivot_alias": ["id", "email"],
                "sep": ";",
                "source1_id": None,
                "source2_id": None,
            },
            "mappingfile": {
                "filename": None,
                "init_path": "C:\Users\jerem\Libraries\projects\datalign\demo\mapping.csv",
                "separator": ";",
            },
            "options": {
                "aggregate_exports": False,
                "aggregate_values": None,
                "debug": True,
                "field_comparison_export_filename": "full_fields_comparison",
                "use_full_id_map": False,
            },
            "source1": {
                "aggregation_field": None,
                "all_pivots": ["id", "mail address"],
                "date_format": "%Y/%m/%d",
                "fields_lbda_trans": ["credit card"],
                "fields_to_filter": None,
                "automerge_field": [
                    [
                        "supervisor id",
                        "mail address",
                        "id",
                    ]
                ],
                "fields_to_merge": [["first name", "last name"]],
                "fields_to_merge_name": ["full name"],
                "dupl_priority_field": None,
                "dupl_priority_value": None,
                "filename": None,
                "get_contry_from_le": False,
                "id_field": "id",
                "init_path": "C:\Users\jerem\Libraries\projects\datalign\demo\dummy_src.csv",
                "lbda_trans": [
                    "lambda x:' '.join([x['credit card'][i:i+4] for i in range(0, len(x), 4)])"
                ],
                "main_id": "id",
                "mapping_col_name": "src",
                "name": "src",
                "pivot_field": "id_key",
                "prefix": "src_",
                "separator": ",",
                "skiprows": 0,
                "values_to_filter": None,
            },
            "source2": {
                "aggregation_field": None,
                "all_pivots": ["employee id", "email"],
                "country_field": None,
                "date_format": "%d-%b-%Y",
                "fields_lbda_trans": ["PhoneNumber"],
                "fields_to_merge": None,
                "fields_to_merge_name": None,
                "dupl_priority_field": None,
                "dupl_priority_value": None,
                "fields_to_filter": None,
                "automerge_field": None,
                "lbda_trans": [
                    "lambda x:x['PhoneNumber'].replace('(','').replace(')','').replace('-','')"
                ],
                "fields_to_filter": None,
                "filename": None,
                "id_field": "employee id",
                "init_path": "C:\Users\jerem\Libraries\projects\datalign\demo\dummy_dest.xlsx",
                "main_id": "employee id",
                "mapping_col_name": "dest",
                "name": "dest",
                "pivot_field": "id_key",
                "prefix": "dest_",
                "separator": ",",
                "skiprows": 0,
                "specific_date_format": True,
                "values_to_filter": None,
            },
        }

        with open(file_path, "w") as config_file:
            yaml.dump(config_data, config_file, default_flow_style=False)
