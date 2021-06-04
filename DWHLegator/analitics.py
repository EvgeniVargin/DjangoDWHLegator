from django.shortcuts import render, get_object_or_404, redirect
from .models import Analitic,Entity,SQLlist
from .forms import AnaliticForm
from .oraConnect import OraConnection
from .utils import IfNoneThenNull

def analitic_list(request):
    analitics = Analitic.objects.all()
    return render(request, 'analitic_list.html', {'analitics': analitics})
    
def analitic_detail(request, pk):
    analitic = get_object_or_404(Analitic, pk=pk)
    return render(request, 'analitic_detail.html', {'analitic': analitic})

def analitic_add(request):
    if request.method == "POST":
        form = AnaliticForm(request.POST)
        if form.is_valid():
            analitic = form.save(commit=False)
            analitic.add(analitic.anlt_code,request.user)
            return redirect('analitic_detail', pk=analitic.pk)
    else:
        form = AnaliticForm()
    return render(request, 'analitic_edit.html', {'form': form})

def analitic_edit(request, pk):
    analitic = get_object_or_404(Analitic, pk=pk)
    try:
        if analitic.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if analitic.checked_out_by is not None and analitic.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if request.method == "POST":
            form = AnaliticForm(request.POST, instance=analitic)
            if form.is_valid():
                analitic = form.save(commit=False)
                analitic.edit(analitic.anlt_code,request.user)
                return redirect('analitic_detail', pk=analitic.pk)
        else:
            form = AnaliticForm(instance=analitic)
            return render(request, 'analitic_edit.html', {'form': form})
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(analitic.anlt_code,analitic.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" Сначала необходимо взять на изменение'%(analitic.anlt_code))
        else:
            print(err.args[0])
        return redirect('analitic_detail', pk=analitic.pk)

def analitic_del(request, pk):
    analitic = get_object_or_404(Analitic, pk=pk)
    try:
        if analitic.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if analitic.checked_out_by is not None and analitic.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        analitic.remove(analitic.anlt_code)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(analitic.anlt_code,analitic.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" Сначала необходимо взять на изменение'%(analitic.anlt_code))
    return render(request, 'analitic_detail.html', {'analitic': analitic})

def analitic_checkout(request, pk):
    analitic = get_object_or_404(Analitic, pk=pk)
    #Если взято на изменение другим пользователем
    try:
        if analitic.checked_out_by is not None and analitic.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
    except NameError:
        print('"%s" Взято на изменение пользователем %s'%(analitic.anlt_code,analitic.checked_out_by))
    if analitic.checked_out_by is None or analitic.checked_out_by == request.user:
        analitic.checkout(analitic.anlt_code,request.user)
    return render(request, 'analitic_detail.html', {'analitic': analitic})

def analitic_checkin(request, pk):
    analitic = get_object_or_404(Analitic, pk=pk)
    #print(SQLlist.objects.filter(sql_name = 'Analitic').first().get_checkin_sql())
    try:
        if analitic.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if analitic.checked_out_by is not None and analitic.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        anltSQL = SQLlist.objects.filter(sql_name = 'Analitic').first().get_checkin_sql()%(analitic.effective_start,analitic.effective_end,analitic.anlt_code,analitic.anlt_name,analitic.archive_flg,analitic.entity_id.entity_name,IfNoneThenNull(analitic.anlt_sql),analitic.anlt_alias,analitic.data_type,analitic.anlt_alias_descr,IfNoneThenNull(analitic.spec_import_sql),analitic.deleted)
        #print(sgnSQL)
        try:
            con = OraConnection('DM_SKB','DWHLegator000000','XE')
            con.exec(anltSQL);
            analitic.checkin(analitic.anlt_code)
        except:
            print('Ошика во время операции CheckIN. Сущность "%s"'%(analitic.analitic_name))
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(analitic.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
        else:
            print(err.args[0])
    if analitic.deleted == 1:
        analitic.delete()
        analitics = Analitic.objects.all()
        return render(request, 'analitic_list.html', {'analitics': analitics})
    else:   
        return redirect('analitic_detail', pk=pk)

def analitic_truncate(request):
    analitics = Analitic.objects.all()
    for rec in analitics:
        rec.delete()
    return render(request, 'analitic_list.html', {'analitics': analitics})

def analitic_import(request):
    con = OraConnection('DM_SKB','DWHLegator000000','XE')
    anltSQL = SQLlist.objects.filter(sql_name = 'Analitic').first().get_checkout_sql_all()

    anltCur = con.get_cursor(anltSQL)

    # Не забываем, что есть dir(), с чьей помощью можно узнать очень
    # много полезного об инстансе курсорной переменной
    #print('grpCur: ', dir(grpCur))
    #print('grpCur.getvalue(): ', dir(grpCur.getvalue()))

    # описание полей запроса
    #for col in con.get_fields(entCur):
    #    print(col)

    for rec in con.get_data(anltCur):
        #try:
        #    Group.objects.filter(group_name = rec[0]).first().delete()
        #except:
        #    None
        anlt = Analitic.objects.filter(anlt_code=rec[2]).first()
        if anlt is None:
            Analitic.objects.create(effective_start = rec[0]
                               ,effective_end = rec[1]
                               ,anlt_code = rec[2]
                               ,anlt_name = rec[3]
                               ,archive_flg = rec[4]
                               ,entity_id = Entity.objects.filter(entity_name = rec[5]).first()
                               ,anlt_sql = rec[6]
                               ,anlt_alias = rec[7]
                               ,data_type = rec[8]
                               ,anlt_alias_descr=rec[9]
                               ,spec_import_sql = rec[10]
                               ,deleted = rec[11])
        else:
            anlt.effective_start = rec[0]
            anlt.effective_end = rec[1]
            anlt.anlt_code = rec[2]
            anlt.anlt_name = rec[3]
            anlt.archive_flg = rec[4]
            anlt.entity_id = Entity.objects.filter(entity_name = rec[5]).first()
            anlt.anlt_sql = rec[6]
            anlt.anlt_alias = rec[7]
            anlt.data_type = rec[8]
            anlt.anlt_alias_descr=rec[9]
            anlt.spec_import_sql = rec[10]
            anlt.deleted = rec[11]
            anlt.save()
    # не забываем закрывать за собой соединение с Ораклом
    con.close()
    analitics = Analitic.objects.all()
    return render(request,'analitic_list.html',{'analitics': analitics})
