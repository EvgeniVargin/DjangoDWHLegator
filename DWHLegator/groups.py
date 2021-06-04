from django.shortcuts import render, get_object_or_404, redirect
from django.utils import timezone
from .models import Group,SQLlist
from .forms import GroupForm
from .oraConnect import *
from .utils import IfNoneThenNull

########################### Группы ############################
def group_list(request):
    groups = Group.objects.all()
    return render(request, 'group_list.html', {'groups': groups})
    
def group_detail(request, pk):
    group = get_object_or_404(Group, pk=pk)
    return render(request, 'group_detail.html', {'group': group})

def group_add(request):
    if request.method == "POST":
        form = GroupForm(request.POST)
        if form.is_valid():
            group = form.save(commit=False)
            group.add(group.group_name,request.user)
            return redirect('group_detail', pk=group.pk)
    else:
        form = GroupForm()
    return render(request, 'group_edit.html', {'form': form})
    
def group_edit(request, pk):
    group = get_object_or_404(Group, pk=pk)
    try:
        if group.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if group.checked_out_by is not None and group.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if group.parent is None:
            grpSQL = SQLlist.objects.filter(sql_name = 'Group').first().get_checkin_sql()%(group.group_name,'',IfNoneThenNull(group.strg_period),IfNoneThenNull(group.strg_period_type,''),group.deleted)
        if request.method == "POST":
            form = GroupForm(request.POST, instance=group)
            if form.is_valid():
                group = form.save(commit=False)
                group.edit(group.group_name,request.user)
                return redirect('group_detail', pk=group.pk)
        else:
            form = GroupForm(instance=group)
            return render(request, 'group_edit.html', {'form': form})
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(group.group_name,group.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" сначала необходимо взять на изменение'%(group.group_name))
        return redirect('group_detail', pk=group.pk)

def group_del(request, pk):
    group = get_object_or_404(Group, pk=pk)
    try:
        if group.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if group.checked_out_by is not None and group.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        group.remove(group.group_name)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(group.group_name,group.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" сначала необходимо взять на изменение'%(group.group_name))
    return render(request, 'group_detail.html', {'group': group})

def group_import(request):
    con = OraConnection('DM_SKB','DWHLegator000000','XE')
    grpSQL = SQLlist.objects.filter(sql_name = 'Group').first().get_checkout_sql_all()

    grpCur = con.get_cursor(grpSQL)

    # Не забываем, что есть dir(), с чьей помощью можно узнать очень
    # много полезного об инстансе курсорной переменной
    #print('grpCur: ', dir(grpCur))
    #print('grpCur.getvalue(): ', dir(grpCur.getvalue()))

    # описание полей запроса
    #for col in con.get_fields(entCur):
    #    print(col)

    for rec in con.get_data(grpCur):
        #try:
        #    Group.objects.filter(group_name = rec[0]).first().delete()
        #except:
        #    None
        Group.objects.create(group_name = rec[0],parent = Group.objects.filter(group_name = rec[1]).first(),strg_period = rec[2],strg_period_type = rec[3])
    # не забываем закрывать за собой соединение с Ораклом
    con.close()
    groups = Group.objects.all()
    return render(request,'group_list.html',{'groups': groups})

def group_truncate(request):
    for rec in Group.objects.all():
        rec.delete()
    groups = Group.objects.all()
    return render(request, 'group_list.html', {'groups': groups})

def group_checkin(request, pk):
    group = get_object_or_404(Group, pk=pk)
    try:
        if group.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if group.checked_out_by is not None and group.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if group.parent is None:
            grpSQL = SQLlist.objects.filter(sql_name = 'Group').first().get_checkin_sql()%(group.group_name,'',IfNoneThenNull(group.strg_period),IfNoneThenNull(group.strg_period_type,''),group.deleted)
        else:
            grpSQL = SQLlist.objects.filter(sql_name = 'Group').first().get_checkin_sql()%(group.group_name,group.parent.group_name,IfNoneThenNull(group.strg_period),IfNoneThenNull(group.strg_period_type,''),group.deleted)
        try:
            con = OraConnection('DM_SKB','DWHLegator000000','XE')
            con.exec(grpSQL);
            group.checkin(group.group_name)
        except NameError as err:
            print(err)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(group.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
        else:
            print(err)
          
    if group.deleted == 1:
        group.delete()
        groups = Group.objects.all()
        return render(request, 'group_list.html', {'groups': groups})
    else:   
        return redirect('group_detail', pk=pk)

def group_checkout(request, pk):
    group = get_object_or_404(Group, pk=pk)
    #Если взято на изменение другим пользователем
    try:
        if group.checked_out_by is not None and group.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
    except NameError:
        print('Взято на изменение пользователем %s'%(group.checked_out_by))
    if group.checked_out_by is None or group.checked_out_by == request.user:
        group.checkout(group.group_name,request.user)
    return render(request, 'group_detail.html', {'group': group})
########################### Окончание Группы ############################
