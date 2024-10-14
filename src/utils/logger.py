from typing import *
from enum import Enum
import contextvars
import traceback
from abc import ABC, abstractmethod
from src.settings import LOGLEVEL
import pendulum as pm


class LogLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    EXCEPTION = 4
    FATAL = 5


def get_system_loglevel() -> LogLevel:
    match LOGLEVEL:
        case "DEBUG": return LogLevel.DEBUG
        case "INFO": return LogLevel.INFO
        case "WARNING": return LogLevel.WARNING
        case "ERROR": return LogLevel.ERROR
        case "EXCEPTION": return LogLevel.EXCEPTION
        case "FATAL": return LogLevel.FATAL
        case _: raise Exception("Invalid LogLevel in settings")


class LoggerScopeABC(ABC):    
    pass

class LoggerABC(ABC):
    @abstractmethod
    def debug(self, message : str):
        pass

    @abstractmethod
    def info(self, message : str):
        pass

    @abstractmethod
    def warning(self, message : str):
        pass

    @abstractmethod
    def error(self, message : str):
        pass

    @abstractmethod
    def exception(self, exception):
        pass

    @abstractmethod
    def make_variant(self, scope : str, level : Optional[LogLevel] = None):
        pass
    
    @abstractmethod
    def set_level(self, level : LogLevel):
        pass

    @abstractmethod
    def create_loggerscope(self, scope : str) -> LoggerScopeABC:
        pass


ctx_scope_stack = contextvars.ContextVar("logger_scope_stack", default=[])
ctx_scope_stack.set([])

class LoggerScope(LoggerScopeABC):    
    def __init__(self, logger : any) -> None:
        self.logger = logger
        pass

    def __enter__(self) -> LoggerABC:
        ctx_scope_stack.set(ctx_scope_stack.get() + [self])
        return self.logger

    def __exit__(self, exc_type, exc_value, traceback):
        try:            
            ctx_scope_stack.get().pop()
        except Exception:
            print("LoggerScope: Stack is empty!")
            pass

    @staticmethod
    def get_current_scope() -> any:
        try:
            if len(ctx_scope_stack.get()) == 0: # Streamlit hack
                ctx_scope_stack.set([LoggerScope(system_logger)])
            return ctx_scope_stack.get()[-1]
        except Exception:            
            # Make some noise
            traceback.print_exc()

            # Return SOMETHING!
            ctx_scope_stack.set([LoggerScope(system_logger)])
            return ctx_scope_stack.get()[-1]
    

class Logger(object):
    def __init__(self, scope : str = None):        
        self.__scope = scope
        self.__LogLevel_to_string_dict = {
            LogLevel.DEBUG :     "DEBUG",
            LogLevel.INFO :      "INFO ",
            LogLevel.WARNING :   "WARN ",
            LogLevel.ERROR :     "ERROR",
            LogLevel.EXCEPTION : "EXCPT",
            LogLevel.FATAL :     "FATAL",
        }
        pass

    def __print_log(self, level : LogLevel, message : str):        
        if self.__scope is not None:
            print("[{} - {}]: {}".format(self.__LogLevel_to_string_dict[level], self.__scope, message))
        else:
            print("[{}] {}".format(self.__LogLevel_to_string_dict[level], message))

    def __current_LogLevel(self) -> LogLevel:
        return get_system_loglevel()

    def debug(self, message : str):
        if self.__current_LogLevel().value > LogLevel.DEBUG.value: return
        self.__print_log(LogLevel.DEBUG, message)

    def info(self, message : str):
        if self.__current_LogLevel().value > LogLevel.INFO.value: return
        self.__print_log(LogLevel.INFO, message)

    def warning(self, message : str):
        if self.__current_LogLevel().value > LogLevel.WARNING.value: return
        self.__print_log(LogLevel.WARNING, message)

    def error(self, message : str):
        if self.__current_LogLevel().value > LogLevel.ERROR.value: return
        self.__print_log(LogLevel.ERROR, message)

    def exception(self, exception):
        if self.__current_LogLevel().value > LogLevel.EXCEPTION.value: return
        self.__print_log(LogLevel.EXCEPTION, exception)

    def make_variant(self, scope : str, level : Optional[LogLevel] = None):
        scope = scope if self.__scope is None else "{}/{}".format(self.__scope, scope)
        return Logger(scope)

    def create_loggerscope(self, scope : str) -> LoggerScope:
        return LoggerScope(self.make_variant(scope))
    

class ScopeLogger(object):
    def __init__(self):
        pass

    @property
    def __logger(self) -> LoggerABC:        
        return LoggerScope.get_current_scope().logger

    def debug(self, message : str):
        self.__logger.debug(f'{pm.now()} {message}')

    def info(self, message : str):
        self.__logger.info(f'{pm.now()} {message}')

    def warning(self, message : str):
        self.__logger.warning(f'{pm.now()} {message}')

    def error(self, message : str):
        self.__logger.error(f'{pm.now()} {message}')

    def exception(self, exception):
        self.__logger.exception(f'{pm.now()}, {exception}')

    def make_variant(self, scope : str):
        return self.__logger.make_variant(scope)
    
    def set_level(self, level : LogLevel):
        self.__logger.set_level(level)

    def create_loggerscope(self, scope : str) -> LoggerScope:
        return LoggerScope(self.__logger.make_variant(scope))


system_logger = Logger()
ctx_scope_stack.set([LoggerScope(system_logger)])
scope_logger = ScopeLogger()