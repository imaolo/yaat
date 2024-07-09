# from yaat.informer import Informer

# informer = Informer(root_path='../', data_path='spy.csv', target='open', d_model=2, n_heads=1, d_ff=2, seq_len=6, label_len=4, pred_len=4, train_epochs=1, features='MS')

# print(informer.train())
# print(informer.test())
# print(informer.predict())

from yaat.maester import Maester
m = Maester()