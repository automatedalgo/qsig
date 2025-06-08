import re

from .base_indicators import BaseIndicator
from .indicator_container import IndicatorContainer
from .generic_indicator import GenericIndicator

RE_PREFIX = re.compile(r'^([\w\s]*=\s*)(\w.*)$')
RE_FUNC1 = re.compile(r'^(\w+)\((.*)\)\[(.*?)\]$')
RE_FUNC2 = re.compile(r'^(\w+)\((.*)\)$')


def _parse_indicator_expression(expr: str):
    expr = expr.strip()
    name = None

    prefix_match = RE_PREFIX.match(expr)
    if prefix_match:
        name = prefix_match[1].strip("= ")
        expr = prefix_match[2]

    matched = RE_FUNC1.match(expr)
    if matched:
        ind_type = matched[1]
        ind_params = [x.strip() for x in matched[2].split(",")]
        ind_source = [x.strip() for x in matched[3].split(",")]
        return name, ind_type, ind_params, ind_source
    matched = RE_FUNC2.match(expr)
    if matched:
        assert len(matched.groups()) == 2
        ind_type = matched[1]
        ind_params = matched[2].split(",") if matched[2] else None
        return name, ind_type, ind_params, None

    raise Exception(f"not valid indicator expression: {expr}")


def _test_indicator_expressions():

    def _test(expr: str):
        expr = ''.join(expr.split())  # remove all whitespace
        ind_name, ind_type, ind_params, ind_source = _parse_indicator_expression(expr)
        ind_params_str = ",".join(ind_params) if ind_params else ""
        rebuild = "{}({})".format(ind_type, ind_params_str)
        if ind_source:
            rebuild += "[" + ",".join(ind_source) + "]"
        if ind_name:
            rebuild = f"{ind_name}={rebuild}"
        assert expr.strip() == rebuild

    _test("FAST = MA()")
    _test("MA() ")
    _test("MA()[]")
    _test("MA(10s)")
    _test("MA(10s)[open]")
    _test("MA(10s,30s)[close]")
    _test("MA(10s,30s)[close]")
    _test(" SLOW=MA(10s,30s)[close,open] ")


class IndicatorFactory:
    """Singleton factory that can create registered indicators"""

    _instance = None
    _initialised = False

    def __init__(self):
        if not self.__class__._initialised:
            self._ind_type_map = dict()
            self.__class__._initialised = True

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @staticmethod
    def instance():
        if IndicatorFactory._instance is None:
            IndicatorFactory._instance = IndicatorFactory()
        return IndicatorFactory._instance

    def register(self,
                 indicator_class: type,
                 indicator_details=None):
        if isinstance(indicator_class, type):
            ind_type = indicator_class.CODE
            assert ind_type not in self._ind_type_map
            self._ind_type_map[ind_type] = indicator_class
        else:
            ind_type = indicator_class
            assert ind_type not in self._ind_type_map
            self._ind_type_map[ind_type] = indicator_details


    def create(self, cls: str, config: dict, container: IndicatorContainer):
        ind_type = cls or config["type"]
        assert isinstance(ind_type, str)
        ind_class = self._ind_type_map.get(ind_type)
        if ind_class is None:
            raise Exception(f"IndicatorFactory doesn't support indicator type '{ind_type}'")

        if issubclass(ind_class, BaseIndicator):
            ind_instance = ind_class.create(config, container)
            return ind_instance
        else:
            details = ind_class
            ind = GenericIndicator.create_from_config(container, ind_type, config, details)
            return ind


    def create_from_expr(self, expr: str, container: IndicatorContainer):
        """Create an indicator from an expression string, eg 'SMA(1m)'"""
        name, ind_type, params, ind_src = _parse_indicator_expression(expr)
        assert isinstance(ind_type, str)
        ind_class = self._ind_type_map.get(ind_type, None)
        if ind_class is None:
            raise Exception(f"IndicatorFactory doesn't support indicator type '{ind_type}'")
        if issubclass(ind_class, BaseIndicator):
            if ind_class is None:
                raise Exception(f"IndicatorFactory doesn't support indicator type '{ind_type}'")
            ind_instance = ind_class.create(None, container, name, params, ind_src)
            return ind_instance
        else:
            details = ind_class
            ind = GenericIndicator.create_from_inline(
                container, name, ind_type, params, ind_src, details)
            return ind


    def list(self):
        """Return list of available indicators"""
        return sorted(self._ind_type_map.keys())
