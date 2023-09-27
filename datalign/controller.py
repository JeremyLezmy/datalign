import tkinter.messagebox as messagebox
import tkinter.filedialog as filedialog
import tkinter as tk
from modelmain import Model
from view import View
import os
import pandas as pd
from CTkMessagebox import CTkMessagebox
import customtkinter as ctk
import threading
import sys
import traceback
from time import sleep


class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View(self)
        self.processing_thread = None
        self.processing_flag = threading.Event()

        self.finished = False
        self.view.report_callback_exception = self.show_error
        self.files = [None, None, None]
        self.configfile = None

    def parse_list_input(self, input_string):
        elements = input_string.split(",")
        elements = [element.strip() for element in elements]
        return elements

    def get_selected_option(self):
        return self.view.main_page.selected_option.get()

    def on_option_select(self, selected_option):
        print("Selected Option:", selected_option)

    def get_left_entry_values(self):
        left_values = {}
        for label_str, entry_var in self.view.main_page.left_entries.items():
            label = label_str.cget("text")
            value = entry_var.get()
            if "," in value and value != ",":
                value = [item.strip() for item in value.split(",")]
            if ";" in value and value != ";":
                value = [item.strip() for item in value.split(";")]
            left_values[label] = value
        return left_values

    def get_right_entry_values(self):
        right_values = {}
        for label_str, entry_var in self.view.main_page.right_entries.items():
            label = label_str.cget("text")
            value = entry_var.get()
            if "," in value and value != ",":
                value = [item.strip() for item in value.split(",")]
            if ";" in value and value != ";":
                value = [item.strip() for item in value.split(";")]
            right_values[label] = value
        return right_values

    def on_drop(self, event, index):
        file_path = event.data
        self.model.set_file(index, file_path)
        self.view.drop_zones[index].configure(
            text=f"File {index+1}: {os.path.basename(file_path)}"
        )
        self.update_drop_zone_color(self.view.main_page.drop_zones[index], file_path)

    def on_drop_click(self, index):
        file_path = filedialog.askopenfilename(
            filetypes=[("CSV and Excel Files", "*.csv;*.xlsx")]
        )
        if file_path:
            self.files[index] = file_path
            self.model.set_file(index, file_path)
            self.view.main_page.drop_zones[index].configure(
                text=f"{os.path.basename(file_path)}"
            )
            self.update_drop_zone_color(
                self.view.main_page.drop_zones[index], file_path
            )

    def on_config_drop_click(self):
        file_path = filedialog.askopenfilename(filetypes=[("Config Files", "*")])
        if file_path:
            self.configfile = file_path
            self.model.set_config_file(file_path)
            self.view.main_page.config_drop_zone.configure(
                text=f"{os.path.basename(file_path)}"
            )
            self.update_drop_zone_color(self.view.main_page.config_drop_zone, file_path)
            self.view.main_page.disable_entries_and_drop_zones()

    def on_config_drop(self, event):
        file_path = event.data
        self.configfile = file_path
        self.model.set_config_file(file_path)
        self.view.main_page.config_drop_zone.configure(
            text=f"{os.path.basename(file_path)}"
        )
        self.view.main_page.config_remove_button.pack()
        self.update_drop_zone_color(self.view.main_page.config_drop_zone, file_path)
        self.view.main_page.disable_entries_and_drop_zones()

    def remove_config_file(self):
        self.model.set_config_file(None)
        self.configfile = None
        self.view.main_page.config_drop_zone.configure(text="Drop / Select file")

        self.update_drop_zone_color(self.view.main_page.config_drop_zone, None)
        self.view.main_page.enable_entries_and_drop_zones()

    def remove_source(self):
        self.model.remove_source_file()
        self.view.main_page.drop_zones[0].configure(text="Drop / Select file")

        self.update_drop_zone_color(self.view.main_page.drop_zones[0], None)

    def remove_dest(self):
        self.model.remove_dest_file()
        self.view.main_page.drop_zones[1].configure(text="Drop / Select file")

        self.update_drop_zone_color(self.view.main_page.drop_zones[1], None)

    def remove_mapping(self):
        self.model.remove_mapping_file()
        self.view.main_page.drop_zones[2].configure(text="Drop / Select file")

        self.update_drop_zone_color(self.view.main_page.drop_zones[2], None)

    def update_drop_zone_color(self, drop_zone, file_path):
        if file_path:
            drop_zone.configure(fg_color="green", hover_color="#0FA05D")
        else:
            drop_zone.configure(
                fg_color=["#3b8ed0", "#1f6aa5"], hover_color=["#36719f", "#144870"]
            )

    def are_entries_valid(self):
        left_values = self.get_left_entry_values()
        right_values = self.get_right_entry_values()

        # Check if any of the entries in the left and right frames are empty
        if any(value in [None, "", [], [""]] for value in left_values.values()):
            return False
        if any(value is [None, "", [], [""]] for value in right_values.values()):
            return False

        return True

    def are_files_valid(self):
        source_file, dest_file, mapping = self.files

        # Check if all three files are provided and have valid extensions
        if not source_file or not dest_file or not mapping:
            return False
        if not source_file.lower().endswith((".csv", ".xlsx")):
            return False
        if not dest_file.lower().endswith((".csv", ".xlsx")):
            return False
        if not mapping.lower().endswith((".csv", ".xlsx")):
            return False

        return True

    def show_log_page(self):
        self.view.main_page.pack_forget()  # Hide the main page
        self.view.log_page.pack(fill=tk.BOTH, expand=True)

    def generate_config_file(self, file_path):
        self.model.generate_config_file(file_path)

    def process_and_generate_report(self):
        self.view.log_page.clear_logs()
        self.view.switch_to_log_page()
        if (self.are_entries_valid() and self.are_files_valid()) or (
            self.configfile is not None
        ):
            if self.processing_thread and self.processing_thread.is_alive():
                CTkMessagebox(
                    title="Processing",
                    message="Processing is already in progress.",
                    icon="warning",
                )
            else:
                self.processing_flag.clear()  # Clear the flag to indicate ongoing processing
                self.processing_thread = threading.Thread(
                    target=self.process_data_and_generate_report
                )
                self.processing_thread_logs = threading.Thread(
                    target=self.read_logs_periodically
                )
                self.processing_thread.start()
                self.processing_thread_logs.start()
                if self.configfile is None:
                    self.model.log.append(f"Start processing.")
                    self.model.log.append("#" * 50)

        elif not self.are_files_valid():
            CTkMessagebox(
                title="Error",
                message="Please load all 3 files with .csv or .xlsx format or the config file.",
                icon="cancel",
            )
        elif not self.are_entries_valid():
            CTkMessagebox(
                title="Error",
                message="Please fill in all entries or provide the config file.",
                icon="cancel",
            )

    def process_data_and_generate_report(self):
        if self.configfile is None:
            left_values = self.get_left_entry_values()
            right_values = self.get_right_entry_values()
            mode = self.get_selected_option()
            self.model.set_mode(mode)
            sleep(0.2)

            for i, f in enumerate(self.files):
                self.model.set_file(i, f)
            sleep(0.2)
            self.model.set_src_args(**left_values)
            sleep(0.2)
            self.model.set_dest_args(**right_values)
            sleep(0.2)

        try:
            if self.configfile is None:
                self.output_file = self.model.process_data()
            else:
                self.view.log_page.update_log(f"Start processing.")
                self.view.log_page.update_log("#" * 50)
                sleep(0.1)
                self.view.log_page.update_log(f"Setting Config file {self.configfile}")
                self.view.log_page.update_progressbar(0.05)

                self.model = Model.cls_from_config_file(configpath=self.configfile)
                sleep(1)

                self.output_file = self.model.process_data(
                    alignmode=self.model.alignobj
                )

        except Exception as e:
            exception_type, exception_object, exception_traceback = sys.exc_info()
            last_frame = traceback.extract_tb(exception_traceback)[-1]
            tb = traceback.extract_tb(exception_traceback)
            message = [f"Exception type: {exception_type.__name__}", "\n"]
            tb = traceback.extract_tb(exception_traceback)
            for frame in tb:
                message.append(
                    f"File name: {frame.filename}({frame.lineno}):\n  {frame.line}"
                    + "\n"
                )

            message.append(f"Error: {e}" + "\n" + "Do you want to close the program?")
            msg = CTkMessagebox(
                title="Error",
                message="".join(message),
                icon="cancel",
                option_1="No",
                option_2="Yes",
                width=700,
            )
            if msg.get() == "Yes":
                self.finished = True
                self.processing_flag.set()
                self.view.after(20, self.view.destroy)
                sleep(1)
                return
            else:
                sleep(1)
                self.finised = True
                self.model._clear_logs()
                self.view.log_page.clear_logs()
                sleep(1)
                self.finished = False
                return

        self.finished = True
        self.model._clear_logs()

        self.success_box(self.output_file)
        self.finished = False
        self.model = Model()

        sleep(0.2)

    def read_logs_periodically(self):
        last_log_index = 0  # Index of the last log read
        sleep(1)
        while not self.finished:
            current_log_index = len(self.model.log)

            # Check if there are new logs to display
            if current_log_index > last_log_index:
                new_logs = self.model.log[last_log_index:]
                last_log_index = current_log_index
                percentage = self.model.progress[-1]

                for log in new_logs:
                    self.view.log_page.update_log(log)
                    self.view.log_page.update_progressbar(percentage)

            # Wait for a short period before checking again
            sleep(0.05)

    def read_logs_periodically_old(self):
        log = self.model.log[-1]

        first = True
        while not self.model.log:
            pass

        while self.model.log and not self.finished:
            if first:
                self.view.log_page.update_log(log)
                first = False
            sleep(0.05)
            if log != self.model.log[-1]:
                self.view.log_page.update_log(self.model.log[-1])
                log = self.model.log[-1]

    def success_box(self, output_file):
        options = [
            "Close app",
            "Open file",
            "Cancel",
        ]

        msg = CTkMessagebox(
            title="Success",
            message=f"Report generated and saved as {output_file}"
            + "\n"
            + "What would you like to do?",
            options=options,
            icon="check",
            width=600,
        )

        choice = msg.get()
        if choice == "Close app":
            self.processing_flag.set()
            self.view.after(20, self.view.destroy)
            sleep(0.7)
        elif choice == "Open file":
            os.system(f'start "excel" {output_file}')
        elif choice == "Cancel":
            pass

    def on_closing(self):
        response = CTkMessagebox(
            title="Exit",
            message="Do you want to close the program?",
            option_1="No",
            option_2="Yes",
            icon="question",
        )
        if response == "Yes":
            self.view.destroy()

    def show_error(self, *args):
        err = traceback.format_exception(*args)
        CTkMessagebox(
            title="Error",
            message=err,
            icon="cancel",
        )
        self.finished = True
