# Reformats input strings from SQL to be python-friendly
class Parser:
    # Reformats select conditions to change grouping variable to current row
    # input_string : SQL-formatted comparison
    # returns : Python-formatted comparison
    def reformat(input_string):
        tokens = list(input_string)
        for i in range(len(tokens)):
            if tokens[i] == '.':
                tokens[i - 1] = 'row'
            if tokens[i] == '=':
                tokens[i] = '=='
        tokens = "".join(tokens)
        return tokens
    # Reformats global having clause to get columns from current row
    # input_string : SQL-formatted comparison
    # columns : list of str
    # returns : Python-formatted comparison
    def reformat_having(input_string, columns):
        tokens = input_string.split(" ")
        for i in range(len(tokens)):
            if "_" + tokens[i] in columns:
                tokens[i] = "row._" + tokens[i]
            if tokens[i] == '=':
                tokens[i] = '=='
        return " ".join(tokens)

# Represents an Attribute being queried in the database.
class Attribute:
    # CONSTRUCTOR
    # column : str
    # aggregation function : None | "avg" | "count" | "sum" | "max" | "min"
    # grouping_var: str
    # str : e.g. 1_sum_quant | sum_quant | quant
    def __init__(self, column, aggregation_function, grouping_var, string):
        self.column = column
        self.aggregation_function = aggregation_function
        self.grouping_var = grouping_var
        self.string = string

        if not aggregation_function in [None, "avg", "count", "sum", "max", "min"]:
            raise ValueError("not a valid aggregation function")
    
    # Construct an Attribute from a list constructed from an input list
    # input : [grouping_var or None; aggregation_function or None; column]
    # str : e.g. 1_sum_quant | sum_quant | quant
    # returns : new Attribute
    def build(input, str):
        if len(input) == 1:
            return Attribute(*input, None, None, str)
        elif len(input) == 2:
            return Attribute(*reversed(input), None, str)
        elif len(input) == 3:
            return Attribute(*reversed(input), str)
    
    # Construct an Attribute from a string input from the project description
    # str : e.g. 1_sum_quant | sum_quant | quant
    # returns : new Attribute
    def build_from_str(str):
        return Attribute.build(str.split("_"), str)

# Represents the relational operator Φ with its six arguments
class EMFQuery:
    # CONSTRUCTOR
    # s : attribute strings separated by ", "
    # n : str of int
    # v : strings separated by ", "
    # f : attribute strings separated by ", "
    # sigma : SQL-formatted comparisons separated by "\n"
    # g : SQL-formatted comparison
    def __init__(self, s, n, v, f, sigma, g):
        self.select_attributes = list(map(lambda x : Attribute.build_from_str(x), s.split(", ")))
        self.num_grouping_variables = int(n)
        self.grouping_attributes = v.split(", ")
        self.f_vect = list(map(lambda x : Attribute.build_from_str(x), f.split(", ")))
        self.select_condition_vect = list(map(lambda x : Parser.reformat(x), sigma.split("\n")))
        self.having_condition = g
    
    # Construct an EMFQuery from the command line
    # returns : new EMFQuery
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
    # CONSTRUCTOR
    # emf : EMFQuery
    def __init__(self, emf):
        self.emf = emf

        # dynamic programming: reuse column values from combinations of grouping variables and columns
        self.column_vals = {}

    # get initial data from SQL database
    def populate_table(self):
        query = f"SELECT * FROM sales"
        table = sql.query(query)
        self.table = pd.DataFrame(table, columns=["cust", "prod", "day", "month", "year", "state", "quant", "date"])

    # construct groups according to the defined group-by attributes
    def group_by(self):
        self.groups = self.table[self.emf.grouping_attributes].drop_duplicates()
        self.data_output = self.groups.copy()
    
    # do a scan of the table for a specific grouping variable (Figure 1)
    # column : str
    # aggregation function : "avg" | "count" | "sum" | "max" | "min"
    # condition : Python-formatted comparison
    # grouping_variable : str
    def aggregate(self, column, aggregation_function, condition, grouping_variable):
        # only aggregate if there is a provided aggregation function
        if aggregation_function == None: return

        # definitions of aggregation function names
        aggregation_functions = {
            "sum": sum,
            "avg": lambda x: sum(x) / len(x),
            "count": len,
            "max": max,
            "min": min
        }

        # build column name with prepended "_"
        new_column_name = aggregation_function + "_" +column
        if grouping_variable != None:
            new_column_name = "_" + str(grouping_variable) + "_" + new_column_name

        print("aggregating " + new_column_name)

        
        if (column, grouping_variable) not in self.column_vals:
            self.column_vals[(column, grouping_variable)] = []
            # for each group: collect all values of column for all matching rows
            # todo?: hella inefficient just rawdogging nested loops
            # possibly sort self.table on the groups first?
            for idx,key in self.groups.iterrows():
                val_list = []
                for _,row in self.table.iterrows():
                    if condition != None and not eval(condition): continue
                    if tuple(row[self.emf.grouping_attributes]) == tuple(key):
                        val_list.append(row[column])
                self.column_vals[(column, grouping_variable)].append(val_list)

        self.data_output[new_column_name] = list(map(aggregation_functions[aggregation_function], self.column_vals[(column, grouping_variable)]))
    
    def aggregate_all(self):
        for f in list(set(self.emf.select_attributes) | set(self.emf.f_vect)):
            self.aggregate(f.column, f.aggregation_function, 
                           self.emf.select_condition_vect[int(f.grouping_var) - 1] if f.grouping_var != None else None,
                           f.grouping_var)
            print(tabulate.tabulate(mf.data_output, headers="keys", tablefmt="psql", showindex=False))

    def global_having_condition(self):
        print("applying having condition")
        if self.emf.having_condition == None: return
        condition = Parser.reformat_having(self.emf.having_condition, self.data_output.columns)

        for idx,row in self.data_output.iterrows():
            if not eval(condition):
                self.data_output.drop(idx, inplace=True)

        print(tabulate.tabulate(mf.data_output, headers="keys", tablefmt="psql", showindex=False))
    
    def clean_up(self):
        print("cleaning up unrequested columns")

        for column in self.data_output.columns:
            column_name = column
            if column_name.startswith("_"): column_name = column_name[1:]
            if column_name not in list(map(lambda x : x.string, self.emf.select_attributes)):
                self.data_output.drop(columns=[column], inplace=True)
            else: self.data_output.rename(columns={column: column_name})
        print(tabulate.tabulate(mf.data_output, headers="keys", tablefmt="psql", showindex=False))

import tabulate



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
mf.aggregate_all()
mf.global_having_condition()
mf.clean_up()