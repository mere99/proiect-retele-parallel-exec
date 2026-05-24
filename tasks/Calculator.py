"""
Clasa de demonstratie pentru executie paralela.
Metoda square(x) simuleaza lucru cu sleep scurt.
"""


class Calculator:
    def square(self, x):
        import time

        time.sleep(0.3)
        return int(x) * int(x)
