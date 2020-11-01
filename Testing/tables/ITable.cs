using System;
using System.Collections.Generic;
using System.Text;

namespace Testing.tables
{
    public interface ITable
    {
        int Id { get; set; }
        int CommonIntValue { get; set; }
        string CommonStringValue { get; set; }
    }
}
