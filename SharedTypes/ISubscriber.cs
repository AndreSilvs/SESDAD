﻿using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD
{
    public interface ISubscriber
    {
        void ReceiveContent(Event evt);
    }
}
