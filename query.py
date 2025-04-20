class Attribute:
    def __init__(self, column, aggregation_function, grouping_var):
        self.column = column
        self.aggregation_function = aggregation_function,
        self.grouping_var = grouping_var
    
    def build(input):
        if len(input) == 1:
            return Attribute(*input, None, None)
        if len(input) == 3:
            return Attribute(*(reversed(input)))
    
    def build_from_str(str):
        return Attribute.build(str.split("_"))

class EMFQuery:
    def __init__(self, s, n, v, f, sigma, g):
        self.select_attributes = list(map(lambda x : Attribute.build_from_str(x), s.split(", ")))
        self.num_grouping_variables = int(n)
        self.grouping_attributes = v.split(", ")
        self.f_vect = list(map(lambda x : Attribute.build_from_str(x), f.split(", ")))
        self.select_condition_vect = sigma.split("\n")
        self.having_condition = g # todo
    
    def build():
        print("SELECT ATTRIBUTE (S):")
        s = input()
        print("NUMBER OF GROUPING VARIABLES (n):")
        n = input()
        print("GROUPING ATTRIBUTES (V):")
        v = input()
        print("F-VECT ([F]):")
        f = input()
        print("SELECT CONDITION-VECT ([σ]):")
        sigma = input()
        print("HAVING CONDITION (G):")
        g = input()

        return EMFQuery(s, n, v, f, sigma, g)

class MFStruct:
    def __init__(self, emf):
        self.emf = emf
        self.table = []
        
    def populate_table(self):
        self.query = list(map(lambda x : x.column, emf.select_attributes))

        self.query = list(set(self.query))
        print(self.query)

emf = EMFQuery(
    "cust, 1_sum_quant, 2_sum_quant, 3_sum_quant",
    "3",
    "cust",
    "1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant",
    """1.state='NY'
    2.state='NJ'
    3.state='CT'""",
    "1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant"
)
MFStruct(emf)