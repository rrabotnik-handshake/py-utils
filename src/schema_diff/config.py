from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    infer_datetimes: bool = False
    color_enabled: bool = True  # Enable colors by default
    show_presence: bool = True

    # derived ANSI codes (empty strings if color disabled)
    def colors(self):
        if not self.color_enabled:
            return "", "", "", "", ""
        return "\033[91m", "\033[92m", "\033[93m", "\033[96m", "\033[0m"
