using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    public interface IPuppetProcess {
        void Status();

        void Freeze();
        void Unfreeze();
        void Crash();
    }
}