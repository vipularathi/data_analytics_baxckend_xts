from datetime import datetime

from QuantLib import Date, Option, Actual365Fixed, India, Settings, PlainVanillaPayoff, EuropeanExercise, \
    EuropeanOption, QuoteHandle, SimpleQuote, YieldTermStructureHandle, FlatForward, BlackVolTermStructureHandle, \
    BlackConstantVol, BlackScholesProcess, AnalyticEuropeanEngine

from common import logger

call_types = ['call', 'CE', 'CA']
put_types = ['put', 'PE', 'PA']


def get_greeks_intraday(spot_price: float, strike_price: float, expiry_date: datetime, option_type: str, option_price: float,
                        calculation_date: datetime = None, volatility: float = 0.20, risk_free_rate: float = None):
    """
    It is used to find option greeks for the given inputs.
    Results are based on the implied volatility found using option price.
    :param spot_price: float
            (S) Spot price
    :param strike_price: float
            (K) Strike price
    :param expiry_date: date
            (T) Time to maturity i.e. expiry date
    :param option_type: str
            Type of option. Possible values: CE, PE
    :param option_price: float
            Price of the option
    :param calculation_date: date
            Observation date. If None is given then present date is taken.
    :param volatility: float
            Annualised Volatility for underlying.
    :param risk_free_rate: float
            Risk free rate to be used by Option Pricing engine.
    :return: tuple(float, float, float, float, float)
            Returns implied volatility, theta, gamma, delta, vega for the option data entered
    """
    maturity_date = Date(expiry_date.day, expiry_date.month, expiry_date.year, expiry_date.hour, expiry_date.minute,
                         expiry_date.second)

    option = None
    if option_type in call_types:
        option = Option.Call
    if option_type in put_types:
        option = Option.Put
    risk_free_rate = 10.0 / 100 if risk_free_rate is None else risk_free_rate
    day_count = Actual365Fixed()
    calendar = India()
    if calculation_date is not None:
        calculation_date = Date(calculation_date.day, calculation_date.month, calculation_date.year, calculation_date.hour,
                                calculation_date.minute, calculation_date.second)
        Settings.instance().evaluationDate = calculation_date

    payoff = PlainVanillaPayoff(option, strike_price)
    exercise = EuropeanExercise(maturity_date)
    european_option = EuropeanOption(payoff, exercise)

    spot_handle = QuoteHandle(SimpleQuote(spot_price))
    flat_ts = YieldTermStructureHandle(FlatForward(calculation_date, risk_free_rate, day_count))
    flat_vol_ts = BlackVolTermStructureHandle(BlackConstantVol(calculation_date, calendar, volatility, day_count))
    bs_process = BlackScholesProcess(spot_handle, flat_ts, flat_vol_ts)

    try:
        # iv = european_option.impliedVolatility(option_price, bs_process)
        iv = european_option.impliedVolatility(targetValue=option_price, process=bs_process, minVol=1.0e-4, maxVol=1000,
                                               accuracy=1.0e-10, maxEvaluations=100)
        # iv = european_option.impliedVolatility(targetValue=option_price, process=bs_process, minVol=0.0001, maxVol=10000000,
        #                                        maxEvaluations=10000000)
        # flat_vol_ts = BlackVolTermStructureHandle(BlackConstantVol(calculation_date, calendar, iv, day_count))
        # bs_process = BlackScholesProcess(spot_handle, flat_ts, flat_vol_ts)

        european_option.setPricingEngine(AnalyticEuropeanEngine(bs_process))
        # bs_price = european_option.NPV()  # NOSONAR
        iv = iv * 100
        theta = european_option.thetaPerDay()
        gamma = european_option.gamma()
        delta = european_option.delta()
        vega = european_option.vega()
        rho = european_option.rho()

    except RuntimeError as exc:
        logger.debug(f'Error in calculating greeks: {exc}')
        iv, theta, gamma, delta, vega, rho = None, None, None, None, None, None
    except TypeError as t_err:
        raise TypeError from t_err

    return {'iv': iv, 'delta': delta, 'theta': theta, 'gamma': gamma, 'vega': vega, 'rho': rho}
