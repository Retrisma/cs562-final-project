# Represents an Attribute being queried in the database.
class Attribute:
    def __init__(self, column, aggregation_function, grouping_var):
        self.column = column
        self.aggregation_function = aggregation_function
        self.grouping_var = grouping_var

        if not aggregation_function in [None, "avg", "count", "sum", "max", "min"]:
            raise ValueError("not a valid aggregation function")
    
    # Construct an Attribute from a list constructed from an input list
    # input : [grouping_var or None; aggregation_function or None; column]
    def build(input):
        if len(input) == 1:
            return Attribute(*input, None, None)
        elif len(input) == 2:
            return Attribute(*reversed(input), None)
        elif len(input) == 3:
            return Attribute(*reversed(input))
    
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

import sql
import pandas as pd

# Represents an "mf-structure" represented in "Evaluation of Ad Hoc OLAP: In-Place Computation"
class MFStruct:
    def __init__(self, emf):
        self.emf = emf

    # get initial data from SQL database
    def populate_table(self):
        #signature = list(set(map(lambda x : x.column, emf.select_attributes)))

        #query = f"SELECT {",".join(signature)} FROM sales"
        query = f"SELECT * FROM sales"
        table = sql.query(query)
        #self.table = pd.DataFrame(table, columns=signature)
        self.table = pd.DataFrame(table, columns=["cust", "prod", "day", "month", "year", "state", "quant", "date"])

    # construct groups according to the defined group-by attributes
    def group_by(self):
        self.groups = self.table[self.emf.grouping_attributes].drop_duplicates()
    
    # do a scan of the table for a specific grouping variable (Figure 1)
    def aggregate(self, column, aggregation_function, condition, grouping_variable):
        # definitions of aggregation function names
        aggregation_functions = {
            "sum": sum,
            "avg": lambda x: sum(x) / len(x),
            "count": len,
            "max": max,
            "min": min
        }

        new_column = []
        # for each group: collect all values of column for all matching rows
        # todo?: hella inefficient just rawdogging nested loops
        # possibly sort self.table on the groups first?
        # also: implement dynamism to store val_list in case multiple aggregation functions on that one column are needed
        for idx,key in self.groups.iterrows():
            val_list = []
            for _,row in self.table.iterrows():
                if condition != None and not eval(condition): continue
                if tuple(row[self.emf.grouping_attributes]) == tuple(key):
                    val_list.append(row[column])
            new_column.append(aggregation_functions[aggregation_function](val_list))

        new_column_name = column + "_" + aggregation_function
        if grouping_variable != None:
            new_column_name = str(grouping_variable) + "_" + new_column_name
        self.groups[new_column_name] = new_column

import tabulate

class Parser:
    def reformat(input_string):
        tokens = list(input_string)
        for i in range(len(tokens)):
            if tokens[i] == '.':
                tokens[i - 1] = 'row'
            if tokens[i] == '=':
                tokens[i] = '=='
        tokens = "".join(tokens)
        return tokens.trim()

emf = EMFQuery(
    "cust, sum_quant, 1_sum_quant, 2_sum_quant, 3_sum_quant",
    "3",
    "cust",
    "1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant",
    """1.state='NY'
    2.state='NJ'
    3.state='CT'""",
    "1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant"
)


mf = MFStruct(emf)
mf.populate_table()

mf.group_by()
mf.aggregate("quant", "avg", Parser.reformat("1.state='NJ'"), 1)

print(tabulate.tabulate(mf.groups, headers="keys", tablefmt="psql"))

