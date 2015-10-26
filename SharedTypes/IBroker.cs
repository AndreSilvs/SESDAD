using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD
{
    public interface IBroker
    {
        void SendContent(Event evt);
    }
}