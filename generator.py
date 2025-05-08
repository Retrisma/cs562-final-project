import subprocess
import query

def main():
    with open("query.py", 'r') as file:
        # Write the generated code to a file
        open("_generated.py", "w").write(file.read())
        # Execute the generated code
        subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
