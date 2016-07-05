#ifndef GPOS_CAUTOPOINTERBASE_H
#define GPOS_CAUTOPOINTERBASE_H

#include "CStackObject.h"

namespace gpos
{
	template<typename T>
	class CAutoPointerBase: public CStackObject
	{
	protected:
		CAutoPointerBase<T>()
		: m_pt(NULL)
		{}

		CAutoPointerBase<T>(T* pt)
		: m_pt(pt)
		{}
		// actual element to point to
		T *m_pt;
	public:

		// deref operator
		T &operator * ()
		{
			GPOS_ASSERT(NULL != m_pt);
			return *m_pt;
		}

		// returns only base pointer, compiler does appropriate deref'ing
		T* operator -> ()
		{
			return m_pt;
		}

		// return basic pointer
		T* Pt()
		{
			return m_pt;
		}

		// unhook pointer from auto object
		T* PtReset()
		{
			T* pt = m_pt;
			m_pt = NULL;
			return pt;
		}
	};
}


#endif //GPOS_CAUTOPOINTERBASE_H
