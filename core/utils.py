class Logger:
    def info(self, msg: str) -> None: print(f"\033[94m[INFO]\033[0m {msg}")
    def success(self, msg: str) -> None: print(f"\033[92m[SUCCESS]\033[0m {msg}")
    def error(self, msg: str) -> None: print(f"\033[91m[ERROR]\033[0m {msg}")
    def debug(self, msg: str) -> None: print(f"\033[90m[DEBUG]\033[0m {msg}")
    def warn(self, msg: str) -> None: print(f"\033[93m[WARNING]\033[0m {msg}")
    def log(self, msg: str) -> None: print(f"[LOG] {msg}")