from .indicator_container import IndicatorContainer
from .base_indicators import BaseIndicator

# GenericIndicator is another way to define indicators.  Rather than include all
# the indicator logic in a class derived from BaseIndicator, with
# GenericIndicator we instead define an indicator details class that has
# components such as how-to-calculate, how-to-parse-config and so on, and then
# combine that details class with a GenericIndicator instance.


class GenericIndicator(BaseIndicator):

    @staticmethod
    def create_from_config(owner: IndicatorContainer, code, config: dict, details):
        name = None
        if "name" in config:
            name = config["name"]

        ind_params, ind_inputs = details.parse_config(config)

        return GenericIndicator(name=name,
                                code=code,
                                owner=owner,
                                params=ind_params,
                                inputs=ind_inputs,
                                details=details)

    @staticmethod
    def create_from_inline(owner: IndicatorContainer, name, code, params, inputs, details):
        # for inline indicators, try to build a short name, using the raw params
        # and inputs
        if name is None:
            if isinstance(params, list):
                name = "{}({})".format(code, ",".join(params))
            else:
                name = "{}({})".format(code, params)
            if inputs is not None:
                if isinstance(inputs, str):
                    name = f"{name}[{inputs}]"
                else:
                    name = "{}[{}]".format(name, ",".join(inputs))

        if inputs is None:
            inputs = ["close"]  # FIXME: use constant

        # parse the inline parameters & inputs strings, to create configs
        params_cfg, inputs_cfg = details.params_to_config(params, inputs)
        config = params_cfg | inputs_cfg

        # parse the config into actual Parameters instance and input-map
        ind_params, ind_inputs = details.parse_config(config)

        return GenericIndicator(name=name,
                                code=code,
                                owner=owner,
                                params=ind_params,
                                inputs=ind_inputs,
                                details=details)


    def __init__(self, name, code, owner, params, inputs, details):
        self._params = params
        self._inputs = inputs  # can be a dict or string
        self._details = details
        self._repr = f"{code}({params})[{inputs}]"

        # name should be provided outside, so this is the actual default
        if name is None:
            name = f"{code}({params})[{inputs}]"

        super().__init__(code, owner, name)


    def __repr__(self):
        return self._repr


    def _compute(self):
        if hasattr(self._details, "Inputs"):
            # If the indicator possess the 'Inputs' attribute, it means it
            # requires multiple inputs.  So we have to resolve them here and
            # pass them to the calc method.
            inputs = dict()
            for name, value in self._inputs.items():
                inputs[name] = self._owner.find(value)
            result = self._details.calc(self._params, self._details.Inputs(**inputs))
        else:
            # The indicator definition didn't have an Inputs field, so, the
            # default behaviour is that it takes a single input
            assert isinstance(self._inputs, str)
            input_data = self._owner.find(self._inputs)
            result = self._details.calc(self._owner, self._params, input_data)

        self._store_result(result)
