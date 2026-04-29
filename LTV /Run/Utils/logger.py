import logging
from datetime import datetime
from pathlib import Path

class SystemLogger:
    """Logger dual (consola + archivo)"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._setup()
        return cls._instance
    
    def _setup(self):
        self.log_dir = Path(__file__).parent.parent.parent / "logs"
        self.log_dir.mkdir(exist_ok=True)
        
        log_file = self.log_dir / f"ltv_system_{datetime.now().strftime('%Y%m%d')}.log"
        
        self.logger = logging.getLogger("LTVSystem")
        self.logger.setLevel(logging.DEBUG)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg, exc_info=False):
        self.logger.error(msg, exc_info=exc_info)
    
    def warning(self, msg):
        self.logger.warning(msg)
    
    def debug(self, msg):
        self.logger.debug(msg)
    
    def decision(self, msg):
        self.logger.info(f"[DECISION] {msg}")