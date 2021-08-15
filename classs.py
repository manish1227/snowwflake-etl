class Employee:
    count=0

    def empcount(self):
      Employee.count+=1

    def displaycount(self):
        print("count",Employee.count)

emp1=Employee()
emp1.displaycount()

