import tkinter as tk
from tkinter import TOP, filedialog
from tkinter import font as Tkfont
from tkinterdnd2 import DND_FILES
import customtkinter as ctk
import os, sys
from tkinterdnd2 import TkinterDnD
from tkinterweb import HtmlFrame
import pathlib


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


class Tk(ctk.CTk, TkinterDnD.DnDWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.TkdndVersion = TkinterDnD._require(self)


ctk.set_appearance_mode("dark")


class RichText(tk.Text):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        default_font = Tkfont.nametofont("TkDefaultFont")
        default_font = Tkfont.nametofont(self.cget("font"))

        em = default_font.measure("m")
        default_size = default_font.cget("size")
        bold_font = ctk.CTkFont(**default_font.configure())
        italic_font = ctk.CTkFont(**default_font.configure())
        h1_font = ctk.CTkFont(**default_font.configure())

        bold_font.configure(weight="bold")
        italic_font.configure(slant="italic")
        h1_font.configure(size=int(default_size * 2), weight="bold")

        self.tag_config("bold", font=bold_font)
        self.tag_config("italic", font=italic_font)
        self.tag_config("h1", font=h1_font, spacing3=default_size)

        lmargin2 = em + default_font.measure("\u2022 ")
        self.tag_config("bullet", lmargin1=em, lmargin2=lmargin2)

    def insert_bullet(self, index, text):
        self.insert(index, f"\u2022 {text}", "bullet")


class WikiPage(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent, fg_color="transparent")
        self.controller = controller

        wiki_html = HtmlFrame(self, width=1000, height=900, vertical_scrollbar=True)
        with open(resource_path("wiki/wiki.html"), "r") as file:
            html_as_string = file.read()

        file_url = pathlib.Path(
            os.path.abspath(resource_path("wiki/wiki.html"))
        ).as_uri()
        wiki_html.load_file(file_url)
        wiki_html.grid_propagate(0)

        wiki_html.pack(pady=40)


class LogPage(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent, fg_color="transparent")
        self.controller = controller
        self.create_widgets()

    def create_widgets(self):
        log_label = ctk.CTkLabel(self, text="Logs", font=("Arial", 16))
        log_label.pack(pady=10)

        self.log_text = ctk.CTkTextbox(self, height=500, width=580)
        self.log_text.pack(padx=10, pady=15)

        self.progress_bar = ctk.CTkProgressBar(
            self, orientation="horizontal", mode="determinate", width=500, height=25
        )
        self.progress_bar.pack(padx=10, pady=0)
        self.progress_bar.set(0)

        self.percentage_label = ctk.CTkLabel(
            self,
            text="0 %",
            font=("Helevetica", 14),
            height=20,
        )
        self.percentage_label.pack(padx=10, pady=5)

    def update_progressbar(self, percentage):
        self.progress_bar.set(percentage)
        self.percentage_label.configure(text=f"{int(percentage*100)} %")

    def update_log(self, log):
        scroll_pos = self.log_text.yview()

        was_at_bottom = self.is_scroll_at_bottom()
        if was_at_bottom:
            scroll_pos = 1.0
        else:
            scroll_pos = self.log_text.yview()

        log_text = self.log_text.get("1.0", tk.END)
        self.log_text.delete("1.0", tk.END)
        log_text += log + "\n"
        self.log_text.insert(tk.END, log_text)
        if was_at_bottom:
            self.log_text.yview_moveto(scroll_pos)
        else:
            self.log_text.yview_moveto(scroll_pos[0])

    def is_scroll_at_bottom(self):
        # Determine if the scroll position is at the bottom
        _, yview_end = self.log_text.yview()
        return yview_end == 1.0

    def clear_logs(self):
        self.log_text.delete("1.0", tk.END)


class MainPage(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent, fg_color="transparent")
        self.controller = controller
        self.left_entries = {}  # Dictionary to store left entry widgets and labels
        self.right_entries = {}  # Dictionary to store right entry widgets and labels

        self.create_widgets()

    def create_widgets(self):
        self.left_frame = ctk.CTkFrame(self)
        self.middle_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.right_frame = ctk.CTkFrame(self)
        self.info_frame = ctk.CTkFrame(self, height=40, fg_color="transparent")

        self.left_frame.grid(row=0, column=0, sticky="nsew", padx=(10, 5), pady=(10, 5))
        self.middle_frame.grid(row=0, column=1, sticky="nsew", padx=10, pady=(10, 5))
        self.right_frame.grid(
            row=0, column=2, sticky="nsew", padx=(5, 10), pady=(10, 5)
        )
        self.info_frame.grid(
            row=1, column=0, columnspan=3, sticky="nsew", padx=10, pady=(0, 5)
        )
        copyright_symbol = "\u00A9"
        self.copyright_label = ctk.CTkLabel(
            self.info_frame,
            text=f"{copyright_symbol} 2023 Jérémy Lezmy",
            font=("Arial", 15, "italic"),
        )
        self.copyright_label.pack(side=tk.LEFT, padx=(10, 0))

        # Adjust the column weights to allocate the remaining space to the middle frame
        total_columns = 3  # Total number of columns
        left_weight = 30
        middle_weight = 100 - 2 * left_weight
        right_weight = left_weight

        self.grid_columnconfigure(0, weight=left_weight)
        self.grid_columnconfigure(1, weight=middle_weight)
        self.grid_columnconfigure(2, weight=right_weight)
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        left_frame_title = ctk.CTkLabel(
            self.left_frame, text="SRC args", font=("Helvetica", 16)
        )
        left_frame_title.pack(pady=30)

        ### left frame arguments (label + entry)

        default_entries_left = {
            "SRC name": "src",
            "Main ID Field": "id",
            "All pivot fields": "id , mail_address, national ID",
            "Date format": "%Y/%m/%d",
            "File skiprows": "2",
            "File separator": ",",
            "Pivot alias": "id , mail , national_id",
        }
        self.left_entry = []
        for label, def_val in default_entries_left.items():
            label = ctk.CTkLabel(self.left_frame, text=label)
            label.pack(fill=tk.X, pady=(5, 0))

            entry_left_var = ctk.StringVar()
            entry_left = ctk.CTkEntry(
                self.left_frame, width=200, height=35, textvariable=entry_left_var
            )
            entry_left.pack(fill=tk.X, padx=5, pady=(0, 15))
            entry_left_var.set(def_val)

            self.left_entries[label] = entry_left_var
            self.left_entry.append(entry_left)
            entry_left_var.trace(
                "w",
                lambda name, index, mode, var=entry_left_var: self.on_entry_change(
                    var, f"Label {i+1}"
                ),
            )

        right_frame_title = ctk.CTkLabel(
            self.right_frame, text="DEST args", font=("Helvetica", 16)
        )
        right_frame_title.pack(
            pady=30,
        )

        default_entries_right = {
            "DEST name": "dest",
            "Main ID Field": "Employee ID",
            "All pivot fields": "Employee ID , email , NationalID",
            "Date format": "%d/%m/%Y",
            "File skiprows": "0",
            "File separator": ",",
        }
        self.right_entry = []
        for label, def_val in default_entries_right.items():
            label = ctk.CTkLabel(self.right_frame, text=label)
            label.pack(fill=tk.X, pady=(5, 0))

            entry_right_var = ctk.StringVar()
            entry_right = ctk.CTkEntry(
                self.right_frame, width=200, height=35, textvariable=entry_right_var
            )
            entry_right.pack(fill=tk.X, padx=5, pady=(0, 15))
            entry_right_var.set(def_val)

            self.right_entries[label] = entry_right_var
            self.right_entry.append(entry_right)
            entry_right_var.trace(
                "w",
                lambda name, index, mode, var=entry_right_var: self.on_entry_change(
                    var, f"Label {i+1}"
                ),
            )

        self.drop_zones = []
        for i, zone_type in enumerate(["Source", "Dest", "Mapping"]):
            zone_frame = ctk.CTkFrame(self.middle_frame, height=100)
            if i == 0:
                zone_frame.pack(pady=(30, 20))
            else:
                zone_frame.pack(pady=(20))

            label_type = ctk.CTkLabel(
                zone_frame,
                text=zone_type,
                height=35,
                width=200,
                font=("CMU Serif Roman", 18),
                corner_radius=90,
            )
            label_type.pack(side=tk.TOP, pady=5)

            drop_zone = ctk.CTkButton(
                zone_frame,
                text=f"Drop / Select file",
                command=lambda idx=i: self.controller.on_drop_click(idx),
                height=35,
                width=200,
                font=("Helvetica", 18),
            )
            drop_zone.pack(side=tk.LEFT, padx=5)

            drop_zone.drop_target_register(DND_FILES)
            drop_zone.dnd_bind(
                "<<Drop>>", lambda event, idx=i: self.controller.on_drop(event, idx)
            )
            self.drop_zones.append(drop_zone)
            self.add_remove_button(zone_frame, zone_type)

        options = ["Basic"]
        self.method_zone = ctk.CTkFrame(self.right_frame, height=70)
        self.method_zone.pack(pady=(10, 5))
        method_label = ctk.CTkLabel(
            self.method_zone,
            text="Method",
            height=25,
            width=100,
            font=("CMU Serif Roman", 15),
            corner_radius=90,
        )
        method_label.pack(side=tk.TOP, pady=(2, 1))
        self.selected_option = ctk.StringVar(value=options[0])  # Default option
        self.option_menu = ctk.CTkOptionMenu(
            self.method_zone,
            width=150,
            height=25,
            values=options,
            variable=self.selected_option,
        )
        self.option_menu.pack(pady=5, padx=5)

        self.config_frame = ctk.CTkFrame(self.middle_frame, height=100)

        self.config_frame.pack(pady=20)

        config_label = ctk.CTkLabel(
            self.config_frame,
            text="Config File (opt)",
            height=35,
            width=200,
            font=("CMU Serif Roman", 18),
            corner_radius=90,
        )
        config_label.pack(side=tk.TOP, pady=5)

        self.config_drop_zone = ctk.CTkButton(
            self.config_frame,
            text="Drop / Select file",
            command=self.controller.on_config_drop_click,
            height=35,
            width=200,
            font=("Helvetica", 18),
        )

        self.config_drop_zone.pack(side=tk.LEFT, padx=5)

        self.config_drop_zone.drop_target_register(DND_FILES)
        self.config_drop_zone.dnd_bind("<<Drop>>", self.controller.on_config_drop)

        self.config_remove_button = ctk.CTkButton(
            self.config_frame,
            text="X",
            command=self.controller.remove_config_file,
            height=25,
            width=25,
            fg_color="red",
            font=("Helvetica", 13),
        )
        self.config_remove_button.pack(pady=10, padx=5)

        process_button = ctk.CTkButton(
            self.middle_frame,
            text="Process and Generate Report",
            command=self.controller.process_and_generate_report,
            width=300,
            height=60,
            font=("Helvetica", 18),
            fg_color="green",
            hover_color="#055731",
        )
        process_button.pack(pady=20)

    def on_entry_change(self, var, label):
        # This function is called whenever an entry value is changed by the user
        # Update the corresponding dictionary entry with the new value
        if label in self.left_entries:
            self.left_entries[label] = var.get()
        elif label in self.right_entries:
            self.right_entries[label] = var.get()

    def add_remove_button(self, frame, kind):
        if kind == "Source":
            command = self.controller.remove_source
        elif kind == "Dest":
            command = self.controller.remove_dest
        elif kind == "Mapping":
            command = self.controller.remove_mapping

        remove_button = ctk.CTkButton(
            frame,
            text="X",
            command=command,
            height=25,
            width=25,
            fg_color="red",
            font=("Helvetica", 13),
        )
        remove_button.pack(pady=10, padx=(0, 5))

    def disable_entries_and_drop_zones(self):
        for entry in self.left_entry:
            entry.configure(state=tk.DISABLED, fg_color="gray", text_color="gray")
        for entry in self.right_entry:
            entry.configure(state=tk.DISABLED, fg_color="gray", text_color="gray")
        for drop_zone in self.drop_zones:
            drop_zone.configure(
                state=tk.DISABLED,
                fg_color="gray",
            )

        self.option_menu.configure(state=tk.DISABLED, fg_color="gray")

    def enable_entries_and_drop_zones(self):
        for entry in self.left_entry:
            entry.configure(
                state=tk.NORMAL, fg_color=["#f9f9fa", "#343638"], text_color="white"
            )
        for entry in self.right_entry:
            entry.configure(
                state=tk.NORMAL, fg_color=["#f9f9fa", "#343638"], text_color="white"
            )
        for drop_zone in self.drop_zones:
            drop_zone.configure(
                state=tk.NORMAL,
                fg_color=["#3b8ed0", "#1f6aa5"],
                hover_color=["#36719f", "#144870"],
                text_color="white",
            )
        self.option_menu.configure(
            state=tk.NORMAL,
            fg_color=["#3b8ed0", "#1f6aa5"],
        )


class View(Tk):
    def __init__(self, controller):
        super().__init__()
        self.title("DatAlign")
        self.controller = controller
        self.setup_geometry()

        button_frame = ctk.CTkFrame(self, fg_color="transparent")
        button_frame.pack(
            fill=tk.X,
            side=tk.TOP,
            pady=3,
        )

        self.main_page = MainPage(self, self.controller)
        self.log_page = LogPage(self, self.controller)
        self.wiki_page = WikiPage(self, self.controller)

        self.main_page.pack(fill=tk.BOTH, expand=True)
        self.log_page.pack(fill=tk.BOTH, expand=True)
        self.wiki_page.pack(fill=tk.BOTH, expand=True)
        self.log_page.pack_forget()
        self.wiki_page.pack_forget()

        self.main_button = ctk.CTkButton(
            button_frame,
            text="Main Page (Selected)",
            command=self.switch_to_main_page,
            width=250,
            height=45,
            fg_color="green",
            hover_color="#055731",
        )

        self.generate_config_button = ctk.CTkButton(
            button_frame,
            text="Generate \n Config template",
            command=self.generate_config,
            width=200,
            height=45,
            fg_color="#8f5209",
            hover_color="#6e3f08",
        )

        self.log_button = ctk.CTkButton(
            button_frame,
            text="Switch to Logs Page",
            command=self.switch_to_log_page,
            width=250,
            height=45,
        )

        self.wiki_button = ctk.CTkButton(
            button_frame,
            text="Switch to Wiki Page",
            command=self.switch_to_wiki_page,
            width=250,
            height=45,
        )

        button_frame.columnconfigure(0, weight=1)
        button_frame.columnconfigure(1, weight=1)
        button_frame.columnconfigure(2, weight=1)
        button_frame.columnconfigure(3, weight=1)
        self.generate_config_button.grid(column=0, row=0, sticky=tk.W, padx=(10, 50))
        self.main_button.grid(column=1, row=0, sticky=tk.W, padx=(4, 4))
        self.log_button.grid(column=2, row=0, sticky=tk.W, padx=(4, 4))
        self.wiki_button.grid(column=3, row=0, sticky=tk.W, padx=(4, 10))

    def setup_geometry(self):
        # Determine the screen width and height
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        print(screen_width, screen_height)
        # Set the desired window width and height (you can adjust these values based on your preference)
        window_width = int(screen_width / 2)
        window_height = int(2.1 * screen_height / 3)

        print(window_width, window_height)
        self.minsize(int(1.1 * screen_width / 3), int(2.1 * screen_height / 3))

        # Calculate the window position to center it on the screen
        x_position = (screen_width - window_width) // 2 - 100
        y_position = (screen_height - window_height) // 2 - 120

        # Set the window geometry to fit the desired width and height
        self.geometry(f"{window_width}x{window_height}+{x_position}+{y_position}")

    def switch_to_main_page(self):
        self.log_page.pack_forget()
        self.wiki_page.pack_forget()
        self.main_page.pack(fill=tk.BOTH, expand=True)
        self.main_button.configure(
            text="Main Page (Selected)",
            fg_color="green",
            hover_color="#055731",
        )
        self.log_button.configure(
            text="Switch to Logs Page",
            fg_color=["#3b8ed0", "#1f6aa5"],
            hover_color=["#36719f", "#144870"],
        )
        self.wiki_button.configure(
            text="Switch to Wiki Page",
            fg_color=["#3b8ed0", "#1f6aa5"],
            hover_color=["#36719f", "#144870"],
        )

    def switch_to_log_page(self):
        self.main_page.pack_forget()
        self.wiki_page.pack_forget()
        self.log_page.pack(fill=tk.BOTH, expand=True)
        self.main_button.configure(
            text="Switch to Main Page",
            fg_color=["#3b8ed0", "#1f6aa5"],
            hover_color=["#36719f", "#144870"],
        )
        self.log_button.configure(
            text="Logs Page (Selected)",
            fg_color="green",
            hover_color="#055731",
        )
        self.wiki_button.configure(
            text="Switch to Wiki Page",
            fg_color=["#3b8ed0", "#1f6aa5"],
            hover_color=["#36719f", "#144870"],
        )

    def switch_to_wiki_page(self):
        self.log_page.pack_forget()
        self.main_page.pack_forget()
        self.wiki_page.pack(fill=tk.BOTH, expand=True)
        self.main_button.configure(
            text="Switch to Main Page",
            fg_color=["#3b8ed0", "#1f6aa5"],
            hover_color=["#36719f", "#144870"],
        )
        self.log_button.configure(
            text="Switch to Logs Page",
            fg_color=["#3b8ed0", "#1f6aa5"],
            hover_color=["#36719f", "#144870"],
        )
        self.wiki_button.configure(
            text="Wiki Page (Selected)",
            fg_color="green",
            hover_color="#055731",
        )

    def switch_page(self):
        if self.log_page.winfo_viewable():  # If the LogPage is visible
            self.log_page.pack_forget()  # Hide the LogPage
            self.main_page.pack(fill=tk.BOTH, expand=True)  # Show the MainPage
            for button in [self.switch_button1, self.switch_button2]:
                if button.cget("text") == "Switch to Main Page":
                    button.configure(text="Switch to Logs")

        elif self.wiki_page.winfo_viewable():  # If the LogPage is visible
            self.wiki_page.pack_forget()  # Hide the LogPage
            self.main_page.pack(fill=tk.BOTH, expand=True)  # Show the MainPage
            for button in [self.switch_button1, self.switch_button2]:
                if button.cget("text") == "Switch to Main Page":
                    button.configure(text="Switch to Logs")

        else:
            self.main_page.pack_forget()  # Hide the MainPage
            self.log_page.pack(fill=tk.BOTH, expand=True)  # Show the LogPage
            for button in [self.switch_button1, self.switch_button2]:
                if button.cget("text") == "Switch to Logs":
                    button.configure(text="Switch to Main Page")

    def update_log(self, log):
        log_text = self.log_text.get("1.0", tk.END)
        self.log_text.delete("1.0", tk.END)
        log_text += log + "\n"
        self.log_text.insert(tk.END, log_text)

    def _quit(self):
        # self.quit()
        self.destroy()

    def generate_config(self):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".yml", filetypes=[("YAML Files", "*.yml")]
        )
        if file_path:
            self.controller.generate_config_file(file_path)
