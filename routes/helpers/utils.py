class Utils:
    @staticmethod
    def unformat_phonenum(phonenum):
        sPhone = str(phonenum.strip())
        if len(sPhone) > 10 :
            s = ''.join(filter(str.isdigit, sPhone))
            # s = sPhone.replace('(','').replace(')','').replace('-','').replace(' ','').replace(',', '')
            return s
        else:
            return phonenum

    @staticmethod
    def validate_phonenum(phonenum):
        length = len(phonenum.strip())
        if (length == 10 ): #does not includes the EOS
            return True
        else:
            return False

    @staticmethod
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i : i+n]
