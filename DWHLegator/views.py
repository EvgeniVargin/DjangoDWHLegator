from django.shortcuts import render, get_object_or_404, redirect
from django.utils import timezone
from django.conf import settings
from .models import Label,SQLlist
from django.contrib.auth.models import User
from django.contrib.auth.views import LoginView,LogoutView
from .forms import LabelForm
from .oraConnect import OraConnection

# Create your views here.
def homepage(request):
    labels = Label.objects.all()
    if len(request.user.username) == 0:
        return redirect('login')
    else:
        return render(request, 'homepage.html', {'labels': labels})

def label_list(request):
    labels = Label.objects.all()
    return render(request, 'label_list.html', {'labels': labels})

def label_detail(request, pk):
    label = get_object_or_404(Label, pk=pk)
    return render(request, 'label_detail.html', {'label': label})

def label_add(request):
    if request.method == "POST":
        form = LabelForm(request.POST)
        if form.is_valid():
            label = form.save(commit=False)
            label.add(label.caption,request.user)
            return redirect('label_detail', pk=label.pk)
    else:
        form = LabelForm()
    return render(request, 'label_edit.html', {'form': form})

def label_edit(request, pk):
    label = get_object_or_404(Label, pk=pk)
    try:
        if label.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if label.checked_out_by is not None and label.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if request.method == "POST":
            form = LabelForm(request.POST, instance=label)
            if form.is_valid():
                label = form.save(commit=False)
                label.edit(label.caption,request.user)
                return redirect('label_detail', pk=label.pk)
        else:
            form = LabelForm(instance=label)
            return render(request, 'label_edit.html', {'form': form})
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('"%s" Взято на изменение пользователем %s'%(label.caption,label.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('"%s" сначала необходимо взять на изменение'%(label.caption))
        else:
            print(err.args[0])
        return redirect('label_detail', pk=label.pk)

def label_del(request, pk):
    label = get_object_or_404(Label, pk=pk)
    try:
        if label.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if label.checked_out_by is not None and label.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        label.remove(label.caption)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(label.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
        else:
            print(err.args[0])
    return render(request, 'label_detail.html', {'label': label})

def label_truncate(request):
    for rec in Label.objects.all():
        rec.delete()
    labels = Label.objects.all()
    return render(request, 'label_list.html', {'labels': labels})

def label_import(request):
    con = OraConnection('DM_SKB','DWHLegator000000','XE')

    labSQL = SQLlist.objects.filter(sql_name = 'Label').first().get_checkout_sql_all()

    labCur = con.get_cursor(labSQL)

    # описание полей запроса
    #for col in con.get_fields(entCur):
    #    print(col)
    # набор данных
    for rec in con.get_data(labCur):
        #try:
        #    Entity.objects.filter(entity_name = rec[0]).first().delete()
        #except:
        #    None
        Label.objects.create(caption = rec[0],ord = rec[1],parent = Label.objects.filter(caption = rec[2]).first(),soft_type = rec[3],view_name = rec[4])
    # не забываем закрывать за собой соединение с Ораклом
    con.close()
    labels = Label.objects.all()
    return render(request, 'label_list.html', {'labels': labels})

def label_checkin(request, pk):
    label = get_object_or_404(Label, pk=pk)
    try:
        if label.checked_out_by is None:
            raise NameError('NoneCheckoutUser')
        if label.checked_out_by is not None and label.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
        if label.parent is None:
            labSQL = SQLlist.objects.filter(sql_name = 'Label').first().get_checkin_sql()%(label.caption,label.ord,'',label.soft_type,label.view_name,label.deleted)
        else:
            labSQL = SQLlist.objects.filter(sql_name = 'Label').first().get_checkin_sql()%(label.caption,label.ord,label.parent.caption,label.soft_type,label.view_name,label.deleted)
        #print(labSQL)
        try:
            con = OraConnection('DM_SKB','DWHLegator000000','XE')
            con.exec(labSQL);
            label.checkin(label.caption)
        except NameError as err:
            print(err)
    except NameError as err:
        if err.args[0] == 'OtherCheckoutUser':
            print('Взято на изменение пользователем %s'%(label.checked_out_by))
        elif err.args[0] == 'NoneCheckoutUser':
            print('Сначала необходимо взять на изменение')
        else:
            print(err.args[0])
    if label.deleted == 1:
        label.delete()
        labels = Label.objects.all()
        return render(request, 'label_list.html', {'labels': labels})
    else:   
        return redirect('label_detail', pk=pk)

def label_checkout(request, pk):
    label = get_object_or_404(Label, pk=pk)
    #Если взято на изменение другим пользователем
    try:
        if label.checked_out_by is not None and label.checked_out_by != request.user.username:
            raise NameError('OtherCheckoutUser')
    except NameError:
        print('Взято на изменение пользователем %s'%(label.checked_out_by))
    if label.checked_out_by is None or label.checked_out_by == request.user:
        label.checkout(label.caption,request.user)
    return render(request, 'label_detail.html', {'label': label})
