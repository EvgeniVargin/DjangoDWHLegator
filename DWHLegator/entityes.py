from django.shortcuts import render, get_object_or_404, redirect
from .models import Entity,SQLlist
from .forms import EntityForm
from .oraConnect import OraConnection
from .utils import IfNoneThenNull

########################### Сущности ############################
def entity_list(request):
    entityes = Entity.objects.all()
    return render(request, 'entity_list.html', {'entityes': entityes})
    
def entity_detail(request, pk):
    entity = get_object_or_404(Entity, pk=pk)
    return render(request, 'entity_detail.html', {'entity': entity})

def entity_add(request):
    if request.method == "POST":
        form = EntityForm(request.POST)
        if form.is_valid():
            entity = form.save(commit=False)
            entity.add(entity.entity_name,request.user)
            return redirect('entity_detail', pk=entity.pk)
    else:
        form = EntityForm()
    return render(request, 'entity_edit.html', {'form': form})
    
def entity_edit(request, pk):
    entity = get_object_or_404(Entity, pk=pk)
    try:
        if entity.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if entity.checked_out_by is not None and entity.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if request.method == "POST":
            form = EntityForm(request.POST, instance=entity)
            if form.is_valid():
                entity = form.save(commit=False)
                entity.edit(entity.entity_name,request.user)
                return redirect('entity_detail', pk=entity.pk)
        else:
            form = EntityForm(instance=entity)
            return render(request, 'entity_edit.html', {'form': form})
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(entity.entity_name,entity.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" сначала необходимо взять на изменение'%(entity.entity_name))
        return redirect('entity_detail', pk=entity.pk)
 
def entity_del(request, pk):
    entity = get_object_or_404(Entity, pk=pk)
    try:
        if entity.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if entity.checked_out_by is not None and entity.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        entity.remove(entity.entity_name)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(entity.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
    return render(request, 'entity_detail.html', {'entity': entity})

def entity_import(request):
    con = OraConnection('DM_SKB','DWHLegator000000','XE')

    entSQL = SQLlist.objects.filter(sql_name = 'Entity').first().get_checkout_sql_all()

    entCur = con.get_cursor(entSQL)

    # описание полей запроса
    #for col in con.get_fields(entCur):
    #    print(col)
    # набор данных
    for rec in con.get_data(entCur):
        #try:
        #    Entity.objects.filter(entity_name = rec[0]).first().delete()
        #except:
        #    None
        Entity.objects.create(entity_name = rec[0],parent = Entity.objects.filter(entity_name = rec[1]).first(),fct_table_name = rec[2],hist_table_name = rec[3],tmp_table_name = rec[4],anlt_flg = rec[5])
    # не забываем закрывать за собой соединение с Ораклом
    con.close()
    entityes = Entity.objects.all()
    return render(request, 'entity_list.html', {'entityes': entityes})

def entity_truncate(request):
    for rec in Entity.objects.all():
        rec.delete()
    entityes = Entity.objects.all()
    return render(request, 'entity_list.html', {'entityes': entityes})

def entity_checkin(request, pk):
    entity = get_object_or_404(Entity, pk=pk)
    try:
        if entity.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if entity.checked_out_by is not None and entity.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if entity.parent is None:
            entSQL = SQLlist.objects.filter(sql_name = 'Entity').first().get_checkin_sql()%(entity.entity_name,'',entity.fct_table_name,entity.hist_table_name,entity.tmp_table_name,entity.anlt_flg,entity.deleted)
        else:
            entSQL = SQLlist.objects.filter(sql_name = 'Entity').first().get_checkin_sql()%(entity.entity_name,entity.parent.entity_name,entity.fct_table_name,entity.hist_table_name,entity.tmp_table_name,entity.anlt_flg,entity.deleted)
        try:
            con = OraConnection('DM_SKB','DWHLegator000000','XE')
            con.exec(entSQL);
            entity.checkin(entity.entity_name)
        except NameError as err:
            print(err)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(entity.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
    if entity.deleted == 1:
        entity.delete()
        entityes = Entity.objects.all()
        return render(request, 'entity_list.html', {'entityes': entityes})
    else:   
        return redirect('entity_detail', pk=pk)

def entity_checkout(request, pk):
    entity = get_object_or_404(Entity, pk=pk)
    #Если взято на изменение другим пользователем
    try:
        if entity.checked_out_by is not None and entity.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
    except NameError:
        print('Взято на изменение пользователем %s'%(entity.checked_out_by))
    if entity.checked_out_by is None or entity.checked_out_by == request.user:
        entity.checkout(entity.entity_name,request.user)
    return render(request, 'entity_detail.html', {'entity': entity})
########################### Окончание Сущности ############################
