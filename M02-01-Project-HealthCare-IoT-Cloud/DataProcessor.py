# this is the base class for RuleAlert and Aggregator class
# the method start need to be overridden in RuleAlert and Aggregator classes
from Aggregator import Aggregator
from RuleAlert import RuleAlert

# this factory class DataProcessor takes charge of creating instances of Aggregator and RuleAlert classes
class DataProcessor:
    #the static method create is responsible for returning an instance of Aggregator class or RuleAlert class based on the program parameter
    @staticmethod
    def create(program):
        try:
            if program.strip().lower() == "aggregates":
                return Aggregator()
            elif program.strip().lower() == "rules":
                return RuleAlert()
            else:
                raise Exception("Parameter program has invalid value, valid values are 'aggregates' and 'rules'")
        except Exception as e:
            raise Exception(str(e))