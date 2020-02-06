from django.shortcuts import render, get_object_or_404

from .models import Employee

# Create your views here.

def employees_list(request):
    employees = Employee.objects.filter().order_by('key')
    return render(request, 'DWHLegator/employees_list.html', {'employees':employees})

def employee_detail(request, pk):
    employees = get_object_or_404(Employee, pk=pk)
    return render(request, 'DWHLegator/employee_detail.html', {'employees': employees})



