# Represents an Attribute being queried in the database.
class Attribute:
    def __init__(self, column, aggregation_function, grouping_var):
        self.column = column
        self.aggregation_function = aggregation_function
        self.grouping_var = grouping_var
    
    # Construct an Attribute from a list constructed from an input list
    # input : [grouping_var or None; aggregation_function or None; column]
    def build(input):
        if len(input) == 1:
            return Attribute(*input, None, None)
        elif len(input) == 2:
            return Attribute(*input, None)
        elif len(input) == 3:
            return Attribute(*(reversed(input)))
    
    # Construct an Attribute from a string input from the project description
    # str : e.g. 1_sum_quant | sum_quant | quant
    def build_from_str(str):
        return Attribute.build(str.split("_"))

# Represents the relational operator Φ with its six arguments
class EMFQuery:
    def __init__(self, s, n, v, f, sigma, g):
        self.select_attributes = list(map(lambda x : Attribute.build_from_str(x), s.split(", ")))
        self.num_grouping_variables = int(n)
        self.grouping_attributes = v.split(", ")
        self.f_vect = list(map(lambda x : Attribute.build_from_str(x), f.split(", ")))
        self.select_condition_vect = sigma.split("\n")
        self.having_condition = g # todo
    
    # Construct an EMFQuery from the command line
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

import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# Represents an "mf-structure" represented in "Evaluation of Ad Hoc OLAP: In-Place Computation"
class MFStruct:
    def __init__(self, emf):
        self.emf = emf
        self.table = []
        self.query = None
        
    def generate_query(self):
        self.query = list(map(lambda x : x.column, emf.select_attributes))

        self.query = list(set(self.query))
        self.query = f"SELECT {",".join(self.query)} FROM sales"

    def populate_table(self):
        load_dotenv()

        user = os.getenv('USER')
        password = os.getenv('PASSWORD')
        dbname = os.getenv('DBNAME')

        conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                                cursor_factory=psycopg2.extras.DictCursor)
        cur = conn.cursor()
        cur.execute(self.query)

        for row in cur:
            self.table.append(row)


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
mf = MFStruct(emf)
mf.generate_query()
mf.populate_table()

print(tabulate.tabulate(mf.table[1:10], headers="keys", tablefmt="psql"))