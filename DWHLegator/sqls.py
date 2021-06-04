from django.shortcuts import render, get_object_or_404, redirect
from .models import SQLlist
from .forms import SQLlistForm
from .oraConnect import *
from .utils import *

########################### SQL-запросы ############################
def sql_list(request):
    sqls = SQLlist.objects.all()
    return render(request, 'sql_list.html', {'sqls': sqls})
    
def sql_detail(request, pk):
    sql = get_object_or_404(SQLlist, pk=pk)
    return render(request, 'sql_detail.html', {'sql': sql})

def sql_add(request):
    if request.method == "POST":
        form = SQLlistForm(request.POST)
        if form.is_valid():
            sql = form.save(commit=False)
            sql.add(sql.sql_name,request.user)
            return redirect('sql_detail', pk=sql.pk)
    else:
        form = SQLlistForm()
    return render(request, 'sql_edit.html', {'form': form})
    
def sql_edit(request, pk):
    sql = get_object_or_404(SQLlist, pk=pk)
    try:
        if sql.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if sql.checked_out_by is not None and sql.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if request.method == "POST":
            form = SQLlistForm(request.POST, instance=sql)
            if form.is_valid():
                sql = form.save(commit=False)
                sql.edit(sql.sql_name,request.user)
                return redirect('sql_detail', pk=sql.pk)
        else:
            form = SQLlistForm(instance=sql)
            return render(request, 'sql_edit.html', {'form': form})
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(sql.sql_name,sql.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" сначала необходимо взять на изменение'%(sql.sql_name))
        return redirect('sql_detail', pk=sql.pk)

def sql_del(request, pk):
    sql = get_object_or_404(SQLlist, pk=pk)
    try:
        if sql.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if sql.checked_out_by is not None and sql.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        sql.remove(sql.sql_name)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(sql.sql_name,sql.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" сначала необходимо взять на изменение'%(sql.sql_name))
    return render(request, 'sql_detail.html', {'sql': sql})

def sql_checkin(request, pk):
    sql = get_object_or_404(SQLlist, pk=pk)
    try:
        if sql.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if sql.checked_out_by is not None and sql.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        try:
            sql.checkin(sql.sql_name)
        except NameError as err:
            print(err)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(sql.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
        else:
            print(err)
          
    if sql.deleted == 1:
        sql.delete()
        sqls = SQLlist.objects.all()
        return render(request, 'sql_list.html', {'sqls': sqls})
    else:   
        return redirect('sql_detail', pk=pk)

def sql_checkout(request, pk):
    sql = get_object_or_404(SQLlist, pk=pk)
    #Если взято на изменение другим пользователем
    try:
        if sql.checked_out_by is not None and sql.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
    except NameError:
        print('Взято на изменение пользователем %s'%(sql.checked_out_by))
    if sql.checked_out_by is None or sql.checked_out_by == request.user:
        sql.checkout(sql.sql_name,request.user)
    return render(request, 'sql_detail.html', {'sql': sql})
########################### Окончание SQL-запросы ############################
