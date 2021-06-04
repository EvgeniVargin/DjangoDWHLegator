from django.shortcuts import render, get_object_or_404, redirect
from .models import Sign,Entity,SQLlist
from .forms import SignForm
from .oraConnect import OraConnection
from .utils import IfNoneThenNull

def sign_list(request):
    signs = Sign.objects.all()
    return render(request, 'sign_list.html', {'signs': signs})
    
def sign_detail(request, pk):
    sign = get_object_or_404(Sign, pk=pk)
    return render(request, 'sign_detail.html', {'sign': sign})

def sign_add(request):
    if request.method == "POST":
        form = SignForm(request.POST)
        if form.is_valid():
            sign = form.save(commit=False)
            sign.add(sign.sign_name,request.user)
            return redirect('sign_detail', pk=sign.pk)
    else:
        form = SignForm()
    return render(request, 'sign_edit.html', {'form': form})

def sign_edit(request, pk):
    sign = get_object_or_404(Sign, pk=pk)
    try:
        if sign.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if sign.checked_out_by is not None and sign.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if request.method == "POST":
            form = SignForm(request.POST, instance=sign)
            if form.is_valid():
                sign = form.save(commit=False)
                sign.edit(sign.sign_name,request.user)
                return redirect('sign_detail', pk=sign.pk)
        else:
            form = SignForm(instance=sign)
            return render(request, 'sign_edit.html', {'form': form})
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(sign.sign_name,sign.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" Сначала необходимо взять на изменение'%(sign.sign_name))
        return redirect('sign_detail', pk=sign.pk)

def sign_del(request, pk):
    sign = get_object_or_404(Sign, pk=pk)
    try:
        if sign.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if sign.checked_out_by is not None and sign.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        sign.remove(sign.sign_name)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(sign.sign_name,sign.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" Сначала необходимо взять на изменение'%(sign.sign_name))
    return render(request, 'sign_detail.html', {'sign': sign})

def sign_checkout(request, pk):
    sign = get_object_or_404(Sign, pk=pk)
    #Если взято на изменение другим пользователем
    try:
        if sign.checked_out_by is not None and sign.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
    except NameError:
        print('"%s" Взято на изменение пользователем %s'%(sign.sign_name,sign.checked_out_by))
    if sign.checked_out_by is None or sign.checked_out_by == request.user:
        sign.checkout(sign.sign_name,request.user)
    return render(request, 'sign_detail.html', {'sign': sign})

def sign_checkin(request, pk):
    sign = get_object_or_404(Sign, pk=pk)
    #print(SQLlist.objects.filter(sql_name = 'Sign').first().get_checkin_sql())
    #return redirect('sign_detail', pk=pk)
    try:
        if sign.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if sign.checked_out_by is not None and sign.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        sgnSQL = SQLlist.objects.filter(sql_name = 'Sign').first().get_checkin_sql()%(sign.sign_name,sign.sign_descr,sign.archive_flg,sign.data_type,sign.hist_flg,sign.entity_id.entity_name,IfNoneThenNull(sign.sign_sql),IfNoneThenNull(sign.mass_sql),IfNoneThenNull(sign.condition),sign.deleted)
        #print(sgnSQL)
        try:
            con = OraConnection('DM_SKB','DWHLegator000000','XE')
            con.exec(sgnSQL);
            sign.checkin(sign.sign_name)
        except:
            print('Ошика во время операции CheckIN. Сущность "%s"'%(sign.sign_name))
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(sign.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
        else:
            print(err.args[0])
    if sign.deleted == 1:
        sign.delete()
        signs = Sign.objects.all()
        return render(request, 'sign_list.html', {'signs': signs})
    else:   
        return redirect('sign_detail', pk=pk)

def sign_truncate(request):
    signs = Sign.objects.all()
    for rec in signs:
        rec.delete()
    return render(request, 'sign_list.html', {'signs': signs})

def sign_import(request):
    con = OraConnection('DM_SKB','DWHLegator000000','XE')
    sgnSQL = SQLlist.objects.filter(sql_name = 'Sign').first().get_checkout_sql_all()

    sgnCur = con.get_cursor(sgnSQL)

    # Не забываем, что есть dir(), с чьей помощью можно узнать очень
    # много полезного об инстансе курсорной переменной
    #print('grpCur: ', dir(grpCur))
    #print('grpCur.getvalue(): ', dir(grpCur.getvalue()))

    # описание полей запроса
    #for col in con.get_fields(entCur):
    #    print(col)

    for rec in con.get_data(sgnCur):
        #try:
        #    Group.objects.filter(group_name = rec[0]).first().delete()
        #except:
        #    None
        sgn = Sign.objects.filter(sign_name=rec[0]).first()
        if sgn is None:
            Sign.objects.create(sign_name = rec[0]
                           ,entity_id = Entity.objects.filter(entity_name = rec[1]).first()
                           ,sign_descr=rec[3]
                           ,archive_flg = rec[4]
                           ,data_type = rec[5]
                           ,hist_flg = rec[6]
                           ,sign_sql = rec[7]
                           ,mass_sql = rec[8]
                           ,condition = rec[9]
                           ,deleted = rec[10])
        else:
            sgn.sign_name = rec[0]
            sgn.entity_id = Entity.objects.filter(entity_name = rec[1]).first()
            sgn.sign_descr=rec[3]
            sgn.archive_flg = rec[4]
            sgn.data_type = rec[5]
            sgn.hist_flg = rec[6]
            sgn.sign_sql = rec[7]
            sgn.mass_sql = rec[8]
            sgn.condition = rec[9]
            sgn.deleted = rec[10]
            sgn.save()
    # не забываем закрывать за собой соединение с Ораклом
    con.close()
    signs = Sign.objects.all()
    return render(request,'sign_list.html',{'signs': signs})
